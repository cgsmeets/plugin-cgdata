/*
 * Copyright (c) 2023, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

/* eslint-disable camelcase */
/* eslint-disable @typescript-eslint/no-unused-vars */
/* eslint-disable no-console */
/* eslint-disable @typescript-eslint/no-unsafe-argument */

import { writeFileSync } from 'node:fs';
import { SfCommand, Flags } from '@salesforce/sf-plugins-core';
import { Messages } from '@salesforce/core';
import { sleep } from '@salesforce/kit';

Messages.importMessagesDirectoryFromMetaUrl(import.meta.url);
const messages = Messages.loadMessages('@salesforce/plugin-cgdata', 'data.move');

export type DataMoveResult = {
  path: string;
};

export type SObject = {
  Name?: string;
  Id?: string;
  CreatedById?: string;
  ULTEST__Orig_Id__c?: string;
  ULTEST__Created_By__c?: string;
}

export default class DataMove extends SfCommand<DataMoveResult> {
  public static readonly summary = messages.getMessage('summary');
  public static readonly description = messages.getMessage('description');
  public static readonly examples = messages.getMessages('examples');

  public static readonly flags = {
    name: Flags.string({
      summary: messages.getMessage('flags.name.summary'),
      description: messages.getMessage('flags.name.description'),
      char: 'n',
      required: true,
    }),
    'target-org': Flags.requiredOrg(),
  };

  public async run(): Promise<DataMoveResult> {
    const { flags } = await this.parse(DataMove);
    const con = flags['target-org'].getConnection('58.0');

    /*
    const lData = new Array<SObject>();
    const rData = await con.query<SObject>('select id from ULTEST__DataTest__c LIMIT 200 OFFSET 800');
    const ids:string[] = [];
    let i = 0;
    for (const f of rData.records) {
      ids.push(f.Id as string);
      f.Name = 'moredata' + i;
      f.ULTEST__Orig_Id__c = f.Id
      f.Id = undefined;
      i++;
      lData.push(f);
    }
    // const res = await con.delete('ULTEST__DataTest2__c',ids);
    const res = await con.insert('ULTEST__DataTest2__c',lData);
    console.log(res[0].errors);
*/

    let job1Done: boolean = false;
    let job2Done: boolean = false;

    const MapCGACCL: Map<string,string> = new Map<string,string>(
      [
        ['ULTEST__DataTest2__c','ULTEST__DataTest__c'],
        ['cgcloud__Fund__c','ACCL__Fund__c'],
        ['cgcloud__Payment__c','ACCL__Payment__c'],
        ['cgcloud__Promotion__c','ACCL__Promotion__c'],
        ['cgcloud__Tactic__c','ACCL__Tactic__c'],
        ['CGT_StandardReport__c','TPM_StandardReport__c'],
        ['CGT_UserCustomerProduct__c','TPM_UserCustomerProduct__c'],
      ]
    );

    const MapCG: Map<string,SObject> = new Map<string,SObject>();
    const MapACCL: Map<string,SObject> = new Map<string,SObject>();


    if (!MapCGACCL.has(flags['name'])) {
      this.log('SUPPORTED OBJECTS');

      let message = '';
      for (const f of MapCGACCL.keys()) message += f +'\n';
      this.log (message);

      this.exit(1);
    }
    const ACCLobject = MapCGACCL.get(flags['name']);

    const acclQuery = 'select Id,CreatedById from ' + ACCLobject + '';
    this.log ('JOB 1:' + acclQuery);
    con.bulk2.pollTimeout = (300_000);
    const job = await con.bulk2.query(acclQuery);
    this.log ('JOB 1 Created');

    const readable = job.stream();
    const chunks: string[] = [];

    readable.on('readable', () => {
      let chunk;
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
      while (null !== (chunk = readable.read())) {
        chunks.push(chunk);
      }
    });

    readable.on('end', () => {
      const content = chunks.join('');

      for (const frow of content.split('\n')) {
        const row = frow.split(',');
        const data: SObject = {Id:row[0], CreatedById:row[1]};
        MapACCL.set(row[0], data);
      }
      job1Done = true;
    });

    const cgQuery = 'select Id,Origin_Id__c from ' + flags['name'] + ' where Origin_Id__c !=null';
    this.log ('JOB 2:' + cgQuery);
    const job2 = await con.bulk2.query(cgQuery);
    this.log ('JOB 2 Created');

    const readable2 = job2.stream();
    const chunks2: string[] = [];

    readable2.on('readable', () => {
      let chunk;
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
      while (null !== (chunk = readable2.read())) {
        chunks2.push(chunk);
      }
    });

    readable2.on('end', () => {
      const content = chunks2.join('');

      for (const frow of content.split('\n')) {
        const row = frow.split(',');
        const data: SObject = {Id:row[0], ULTEST__Orig_Id__c:row[1]};
        MapCG.set(row[0], data);

      }
      MapCG.delete('"Id"');
      MapCG.delete('');
      this.log('Job 2 Done');
      job2Done = true;

    });

    while (!(job1Done && job2Done)) {
      this.log('Processing');
      if (readable.readableDidRead) this.log('Job1 Read');
      if (readable2.readableDidRead) this.log('Job2 Read');
      // eslint-disable-next-line no-await-in-loop
      await sleep(5000);
    }
    this.log('Generate output.csv');

    const fchunks: string[] = [];
    for (const f of MapCG.values()) {
      fchunks.push(f.Id + ',' + MapACCL.get(f.ULTEST__Orig_Id__c as string)?.CreatedById);

    }
    writeFileSync(flags['name'] + '.csv', '"Id","CreatedById"\n' + fchunks.join('\n'));

    this.log('Process Done. Check output.csv');

    return {
      path: '/Users/ksmeets/Local/Projects/plugin-cgdata/src/commands/data/move.ts',
    };
  }
}
