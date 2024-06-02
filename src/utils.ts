import * as _ from 'lodash';

// Convert a string to snake_case
export function toSnakeCase(str: string): string {
  return _.snakeCase(str);
}

// Required headers checklist
export const requiredHeaders = [
  'name',
  'industry',
  'isic',
  'lei',
  'company_url',
  'source_reports_page',
  'hq_country',
  // 'sbt_status',
  // 'sbt_near_term_year',
  // 'sbt_near_term_target',
  // 'net_zero_year',
];

// Check for missing headers
export function checkHeaders(headers: string[]): string[] {
  const missingHeaders = requiredHeaders.filter(header => !headers.includes(header));
  return missingHeaders;
}