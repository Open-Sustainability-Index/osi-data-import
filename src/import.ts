import * as fs from 'fs';
import { Client } from 'pg';
import * as dotenv from 'dotenv';
import { toSnakeCase, checkHeaders, tableHeaders, TableHeader, dateAsISO } from './utils';
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

async function importCsvFileToPostgres(
  client: Client,
  tableName: string,
  requiredHeaders: TableHeader[],
  csvFilePath = './data/companies.csv',
  previewField: string,
  filter?: (filteredRow: Record<string, any>) => boolean
) {
  return new Promise(async (resolve, reject) => {
    const results: any[] = [];

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
      .on('data', (data: any) => results.push(data))
      .on('end', async () => {
        try {
          let rowIndex = 0;
          for (const row of results) {
            rowIndex++;
            try {
              // Filter and map row data to required headers
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
              const columns = Object.keys(filteredRow).join(', ');
              const values = Object.values(filteredRow);
              const valuePlaceholders = values.map((_, i) => `$${i + 1}`).join(', ');
              if (filter === undefined || filter(row) === true) {
                const query = `INSERT INTO ${tableName} (${columns}) VALUES (${valuePlaceholders})`;
                await client.query(query, values);
                // console.log(`Imported ${tableName}:`, row[previewField]);
              } else {
                // console.log(`Skipped ${tableName}:`, row[previewField]);
              }
            } catch (rowError: any) {
              console.warn(`Error inserting '${tableName}':`, rowIndex, `'${row[previewField]}'`, rowError?.message);
              if (rowError?.message.includes('invalid input syntax')) {
                console.warn('Row:', row);
              }
              if (rowError?.message.includes('Client was closed and is not queryable')) {
                reject(rowError);
              }
            }
          }
          console.log(`Data for '${tableName}' imported successfully.\n`);
          resolve('Data imported successfully.');
        } catch (finalError) {
          console.error('Error inserting data:', finalError);
          reject(finalError);
        }
      });
  })
  
}

importAll();
