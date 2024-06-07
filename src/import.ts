import * as fs from 'fs';
import { Client } from 'pg';
import * as dotenv from 'dotenv';
import { toSnakeCase, checkHeaders, tableHeaders, TableHeader, dateAsISO, createInsertMultipleQuery, chunkArray, prettyPostgresError } from './utils';
const csv = require('csv-parser'); // Use require instead of import

dotenv.config();

async function connectDatabase() {
if (!process.env.DATABASE_URL) {
  console.error('DATABASE_URL is not set in the environment variables.');
    process.exit(1);
  }
  
  const client = new Client({
    connectionString: process.env.DATABASE_URL
  });
  
  client.connect();
  return client;
}

async function deleteAllData(client: Client, table = 'all') {
  console.log('Deleting all data...');
  if (table === 'target' || table === 'all') await deleteFromTable(client, 'target');
  if (table === 'commitment' || table === 'all') await deleteFromTable(client, 'commitment');
  if (table === 'emission' || table === 'all') await deleteFromTable(client, 'emission');
  if (table === 'company' || table === 'all') await deleteFromTable(client, 'company');
  console.log('Deletion completed.\n');
}

async function importAll(table = 'all') {
  const client = await connectDatabase();
  await deleteAllData(client, table);
  if (table === 'company' || table === 'all') await importCsvFileToPostgres(client, 'company', tableHeaders.company, './data/companies.csv', 'Slug', (row) => row.Dupe === '');
  if (table === 'emission' || table === 'all') await importCsvFileToPostgres(client, 'emission', tableHeaders.emission, './data/emissions.csv', 'company_slug', (row) => row.year !== '');
  if (table === 'target' || table === 'all') await importCsvFileToPostgres(client, 'target', tableHeaders.target, './data/targets.csv', 'company_slug', (row) => row.Action === 'Target');
  if (table === 'commitment' || table === 'all') await importCsvFileToPostgres(client, 'commitment', tableHeaders.commitment, './data/targets.csv', 'company_slug', (row) => row.Action === 'Commitment');
  await client.end();
}

async function deleteFromTable(client: Client, table: string) {
  console.log(`Deleting data from ${table}...`);
  await client.query(`DELETE FROM ${table};`);
}

function formatRow(row: Record<string, any>, requiredHeaders: TableHeader[]) {
  const filteredRow = Object.keys(row).reduce((acc: Record<string, any>, header) => {
    const snakeCaseHeader = toSnakeCase(header);
    if (requiredHeaders.map(header => header.name).includes(snakeCaseHeader) && row[header] !== undefined) { // Use the original header name to access the row data
      const requiredHeader = requiredHeaders.find(header => header.name === snakeCaseHeader);
      const valueTrimmed = row[header] === 'NA' ? '' : row[header].trim();
      const valueFormatted = valueTrimmed === ''
        ? null
        : requiredHeader ?.type === 'integer'
          ? parseInt(valueTrimmed.replace(/,/g, ''))
          : requiredHeader ?.type === 'float'
              ? parseFloat(valueTrimmed.replace(/,/g, ''))
              : requiredHeader ?.type === 'date'
                ? dateAsISO(valueTrimmed)
                : valueTrimmed;
      acc[snakeCaseHeader] = valueFormatted;
    }
    return acc;
  }, {} as Record<string, any>);
  return filteredRow;
};

async function importCsvFile(
  tableName: string,
  requiredHeaders: TableHeader[],
  csvFilePath = './data/companies.csv',
  previewField: string,
  filter?: (filteredRow: Record<string, any>) => boolean
): Promise<any[]> {
  return new Promise(async (resolve, reject) => {
    const rows: any[] = [];

    fs.createReadStream(csvFilePath)
      .pipe(csv())
      .on('headers', (headers: string[]) => {
        const snakeCaseHeaders = headers.map(toSnakeCase);
        const missingHeaders = checkHeaders(requiredHeaders, snakeCaseHeaders);
  
        if (missingHeaders.length > 0) {
          console.error(`Missing headers: ${missingHeaders.join(', ')}`);
          process.exit(1);
        }
  
        // console.log('Headers in CSV:', snakeCaseHeaders);
      })
      .on('data', (row: any) => {
        if (filter === undefined || filter(row) === true) {
          // console.log(`Imported ${tableName}:`, row[previewField]);
          rows.push(row);
        } else {
          // console.log(`  Skipped ${tableName}:`, row[previewField]);
        }
      })
      .on('end', async () => {
        resolve(rows);
      });
  })
};

async function importCsvFileToPostgres(
  client: Client,
  tableName: string,
  requiredHeaders: TableHeader[],
  csvFilePath = './data/companies.csv',
  previewField: string,
  filter?: (filteredRow: Record<string, any>) => boolean
) {
    console.log(`---------- Start import: '${tableName}' ----------`);
    const rows = await importCsvFile(tableName, requiredHeaders, csvFilePath, previewField, filter);
    const formattedRows = rows.map(row => formatRow(row, requiredHeaders));
    console.log(`'${tableName}' rows:`, formattedRows.length);
    if (formattedRows.length === 0) {
      console.log(`No data for '${tableName}' to import.\n`);
      return;
    }
    const chunks = chunkArray(formattedRows, 100);
    for (const chunk of chunks) {
      const query = createInsertMultipleQuery(tableName, chunk);
      try {
        await client.query(query);
      } catch (error: any) {
        console.error('🚨', prettyPostgresError(error), '//', error?.detail?.substring(0, 100));
      }
    }
    console.log(`Done: '${tableName}'\n`);
}

importAll();
