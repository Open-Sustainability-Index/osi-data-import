import * as _ from 'lodash';
import { QueryConfig } from 'pg';

// Convert a string to snake_case
export function toSnakeCase(str: string): string {
  return _.snakeCase(str);
}

export interface TableHeader {
  name: string;
  type?: 'string' | 'integer' | 'float' | 'date' | 'boolean';
}

// Required headers checklist
export const tableHeaders: Record<string, TableHeader[]> = {
  company: [
    { name: 'name' },
    { name: 'slug' },
    { name: 'industry' },
    // { name: 'isic' },
    { name: 'lei' },
    { name: 'company_url' },
    { name: 'hq_country' },
  ],
  emission: [
    { name: 'company_slug' },
    { name: 'year', type: 'integer' },
    { name: 'fiscal_year' },
    { name: 'scope_1', type: 'float' },
    { name: 'scope_2_market_based', type: 'float' },
    { name: 'scope_2_location_based', type: 'float' },
    { name: 'scope_2_unknown', type: 'float' },
    { name: 'total_scope_3', type: 'float' },
    { name: 'total_emission_market_based', type: 'float' },
    { name: 'total_emission_location_based', type: 'float' },
    { name: 'total_reported_emission_scope_1_2', type: 'float' },
    { name: 'total_reported_emission_scope_1_2_3', type: 'float' },
    { name: 'cat_1', type: 'float' },
    { name: 'cat_2', type: 'float' },
    { name: 'cat_3', type: 'float' },
    { name: 'cat_4', type: 'float' },
    { name: 'cat_5', type: 'float' },
    { name: 'cat_6', type: 'float' },
    { name: 'cat_7', type: 'float' },
    { name: 'cat_8', type: 'float' },
    { name: 'cat_9', type: 'float' },
    { name: 'cat_10', type: 'float' },
    { name: 'cat_11', type: 'float' },
    { name: 'cat_12', type: 'float' },
    { name: 'cat_13', type: 'float' },
    { name: 'cat_14', type: 'float' },
    { name: 'cat_15', type: 'float' },
    { name: 'all_cats' },
    { name: 'upstream_scope_3', type: 'float' },
    { name: 'share_upstream_of_scope_3', type: 'float' },
    { name: 'scope_1_share_of_total_upstream_emissions', type: 'float' },
    { name: 'total_upstream_emissions', type: 'float' },
    { name: 'revenue', type: 'float' },
    { name: 'currency' },
    { name: 'revenue_million', type: 'float' },
    { name: 'cradle_to_gate', type: 'float' },
    { name: 'ghg_standard' },
    { name: 'emission_intensity', type: 'float' },
    { name: 'emission_page' },
    { name: 'page_revenue' },
    { name: 'publication_date', type: 'date' },
    { name: 'source_emissions_page_move' },
    { name: 'source_emission_link' },
    { name: 'source_emission_report' },
    { name: 'source_revenue' },
    { name: 'source_revenue_link' },
    { name: 'status' },
  ],
  target: [
    { name: 'company_slug' },
    // { name: 'lei' },
    { name: 'action' },
    { name: 'full_target_language' },
    { name: 'company_temperature_alignment' },
    { name: 'target' },
    { name: 'target_wording' },
    { name: 'scope' },
    { name: 'target_value', type: 'float' },
    { name: 'type' },
    { name: 'sub_type' }, // TODO: fix this header
    { name: 'target_classification' },
    { name: 'base_year', type: 'integer' },
    { name: 'target_year', type: 'integer' },
    { name: 'year_type' },
    { name: 'date_published', type: 'date' },
  ],
  commitment: [
    { name: 'company_slug' },
    { name: 'action' },
    { name: 'commitment_type' },
    { name: 'commitment_deadline', type: 'date' },
    { name: 'status' },
    { name: 'reason_for_commitment_extension_or_removal' },
    { name: 'year_type' },
    { name: 'date_published', type: 'date' },
  ]
};

// Check for missing headers
export function checkHeaders(requiredHeaders: TableHeader[], headers: string[]): string[] {
  const missingHeaders = requiredHeaders.filter(header => !headers.includes(header.name));
  return missingHeaders.map(header => header.name);
}

export const dateAsISO = (date: Date | string): string | undefined => (date !== null && date !== undefined)
  ? typeof date === 'string' && date.includes('/')
    ? (new Date(date.split('/').reverse().join('-'))).toISOString()
    : (new Date(date)).toISOString()
  : undefined;

export const formatDate = (dateObj: Date): string => `${dateObj.getFullYear()}-${('0' + (dateObj.getMonth() + 1).toString()).slice(-2)}-${('0' + dateObj.getDate().toString()).slice(-2)}`

function makeParameterString(columnCount: number, rowCount: number) {
  return Array.from({ length: rowCount }, (_, i) => `(${Array.from({ length: columnCount }, (_, j) => `$${i * columnCount + j + 1}`).join(', ')})`).join(', ')
}

export const createInsertMultipleQuery = (tableName: string, rows: any[]): QueryConfig => {
  const firstRow = rows[0];
  const headers = Object.keys(firstRow);
  const parameterPlaceholderString = makeParameterString(headers.length, rows.length);
  const values = rows.reduce((acc, row) => {
    acc.push(...Object.values(row))
    return acc
  }, [])
  const query = {
    text: `INSERT INTO ${tableName} (${headers.join(', ')})
    VALUES ${parameterPlaceholderString}`,
    values
  }
  return query;
}

export function chunkArray(array: any[], chunkSize = 5000) {
  const chunks = [];
  for (let i = 0; i < array.length; i += chunkSize) {
    chunks.push(array.slice(i, i + chunkSize));
  }
  return chunks;
}

export function prettyPostgresError(error: any): string {
  return error?.message
    ?.replace(/duplicate key value violates unique constraint "([^"]+)"/, 'Duplicate: ' + error?.detail?.replace(/Key \(([^)]+)\)=\(([^)]+)\) already exists/, '($2)'))
    .replace(/null value in column "([^"]+)" of relation "([^"]+)" violates not-null constraint/, 'Null value in column "$1": ' + error?.detail?.replace(/Failing row contains \(([^)]+)\)\./, '($1)'));
}
