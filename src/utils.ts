import * as _ from 'lodash';

// Convert a string to snake_case
export function toSnakeCase(str: string): string {
  return _.snakeCase(str);
}

export interface TableHeader {
  name: string;
  type?: 'string' | 'number' | 'date' | 'boolean';
}

// Required headers checklist
export const tableHeaders: Record<string, TableHeader[]> = {
  company: [
    { name: 'name' },
    { name: 'industry' },
    // { name: 'isic' },
    { name: 'lei' },
    { name: 'company_url' },
    { name: 'source_reports_page' },
    { name: 'hq_country' },
    // { name: 'sbt_status' },
    // { name: 'sbt_near_term_year' },
    // { name: 'sbt_near_term_target' },
    // { name: 'net_zero_year' },
  ],
  emission: [
    { name: 'company_name' },
    { name: 'year' },
    { name: 'fiscal_year' },
    { name: 'industry' },
    { name: 'isic_rev_4' },
    { name: 'hq_country_move' },
    { name: 'scope_1', type: 'number' },
    { name: 'scope_2_market_based', type: 'number' },
    { name: 'scope_2_location_based', type: 'number' },
    { name: 'scope_2_unknown', type: 'number' },
    { name: 'total_scope_3', type: 'number' },
    { name: 'total_emission_market_based' },
    { name: 'total_emission_location_based' },
    { name: 'total_reported_emission_scope_1_2', type: 'number' },
    { name: 'total_reported_emission_scope_1_2_3', type: 'number' },
    { name: 'cat_1', type: 'number' },
    { name: 'cat_2', type: 'number' },
    { name: 'cat_3', type: 'number' },
    { name: 'cat_4', type: 'number' },
    { name: 'cat_5', type: 'number' },
    { name: 'cat_6', type: 'number' },
    { name: 'cat_7', type: 'number' },
    { name: 'cat_8', type: 'number' },
    { name: 'cat_9', type: 'number' },
    { name: 'cat_10', type: 'number' },
    { name: 'cat_11', type: 'number' },
    { name: 'cat_12', type: 'number' },
    { name: 'cat_13', type: 'number' },
    { name: 'cat_14', type: 'number' },
    { name: 'cat_15', type: 'number' },
    { name: 'all_cats', type: 'number' },
    { name: 'upstream_scope_3', type: 'number' },
    { name: 'share_upstream_of_scope_3', type: 'number' },
    { name: 'scope_1_share_of_total_upstream_emissions', type: 'number' },
    { name: 'total_upstream_emissions', type: 'number' },
    { name: 'revenue', type: 'number' },
    { name: 'currency' },
    { name: 'revenue_million', type: 'number' },
    { name: 'cradle_to_gate', type: 'number' },
    { name: 'ghg_standard' },
    { name: 'emission_intensity' },
    { name: 'emission_page' },
    { name: 'page_revenue' },
    { name: 'publication_date' },
    { name: 'source_emisions_page_move' },
    { name: 'source_emission_link' },
    { name: 'source_emission_report' },
    { name: 'source_revenue' },
    { name: 'source_revenue_link' },
    { name: 'status' },
  ]
};

// Check for missing headers
export function checkHeaders(requiredHeaders: TableHeader[], headers: string[]): string[] {
  const missingHeaders = requiredHeaders.filter(header => !headers.includes(header.name));
  return missingHeaders.map(header => header.name);
}