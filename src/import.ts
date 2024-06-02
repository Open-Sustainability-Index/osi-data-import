import * as fs from 'fs';
import { Client } from 'pg';
import * as dotenv from 'dotenv';
import { toSnakeCase, checkHeaders, tableHeaders, TableHeader } from './utils';
const csv = require('csv-parser'); // Use require instead of import

dotenv.config();

const DATABASE_URL = process.env.DATABASE_URL;

if (!DATABASE_URL) {
  console.error('DATABASE_URL is not set in the environment variables.');
  process.exit(1);
}

const client = new Client({
  connectionString: DATABASE_URL
});

client.connect();

async function deleteAllData() {
  console.log('Deleting all data...');
  await client.query('DELETE FROM target;');
  await client.query('DELETE FROM emission;');
  await client.query('DELETE FROM commitment;');
  // await client.query('DELETE FROM company;');
}

async function importAll() {
  await deleteAllData();
  // await importCsvFileToPostgres(client, 'company', tableHeaders.company, './data/companies.csv');
  await importCsvFileToPostgres(client, 'emission', tableHeaders.emission, './data/emissions.csv', 'company_name');
}

async function importCsvFileToPostgres(
  client: Client,
  tableName: string,
  requiredHeaders: TableHeader[],
  csvFilePath = './data/companies.csv',
  previewField = 'name',
) {
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
                const value = row[header] === ''
                  ? null
                  : requiredHeader ?.type === 'integer'
                    ? parseInt(row[header])
                    : requiredHeader ?.type === 'float'
                        ? parseFloat(row[header])
                        : row[header];
                acc[snakeCaseHeader] = value;
              }
              return acc;
            }, {} as Record<string, any>);

            const columns = Object.keys(filteredRow).join(', ');
            const values = Object.values(filteredRow);
            const valuePlaceholders = values.map((_, i) => `$${i + 1}`).join(', ');

            const query = `INSERT INTO ${tableName} (${columns}) VALUES (${valuePlaceholders})`;
            await client.query(query, values);
            //console.log(`Imported ${tableName}:`, filteredRow[previewField]);
          } catch (error: any) {
            console.warn('Error inserting row:', rowIndex, error?.message);
          }
        }

        console.log('Data imported successfully.');
      } catch (error) {
        console.error('Error inserting data:', error);
      } finally {
        client.end();
      }
    });
}

importAll();
