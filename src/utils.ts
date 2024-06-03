import * as _ from 'lodash';

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
    { name: 'industry' },
    // { name: 'isic' },
    { name: 'lei' },
    { name: 'company_url' },
    { name: 'hq_country' },
  ],
  emission: [
    { name: 'company_name' },
    { name: 'year' },
    { name: 'fiscal_year' },
    { name: 'scope_1', type: 'float' },
    { name: 'scope_2_market_based', type: 'float' },
    { name: 'scope_2_location_based', type: 'float' },
    { name: 'scope_2_unknown', type: 'float' },
    { name: 'total_scope_3', type: 'float' },
    { name: 'total_emission_market_based' },
    { name: 'total_emission_location_based' },
    { name: 'total_reported_emission_scope_1_2', type: 'float' },
    { name: 'total_reported_emission_scope_1_2_3', type: 'integer' },
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
    { name: 'all_cats', type: 'float' },
    { name: 'upstream_scope_3', type: 'float' },
    { name: 'share_upstream_of_scope_3', type: 'float' },
    { name: 'scope_1_share_of_total_upstream_emissions', type: 'float' },
    { name: 'total_upstream_emissions', type: 'integer' },
    { name: 'revenue', type: 'float' },
    { name: 'currency' },
    { name: 'revenue_million', type: 'float' },
    { name: 'cradle_to_gate', type: 'float' },
    { name: 'ghg_standard' },
    { name: 'emission_intensity' },
    { name: 'emission_page' },
    { name: 'page_revenue' },
    { name: 'publication_date' },
    { name: 'source_emissions_page_move' },
    { name: 'source_emission_link' },
    { name: 'source_emission_report' },
    { name: 'source_revenue' },
    { name: 'source_revenue_link' },
    { name: 'status' },
  ],
  target: [
    { name: 'company_name' },
    // { name: 'lei' },
    { name: 'action' },
    { name: 'full_target_language' },
    { name: 'company_temperature_alignment' },
    { name: 'target' },
    { name: 'target_wording' },
    { name: 'scope' },
    { name: 'target_value' },
    { name: 'type' },
    { name: 'sub_type' }, // TODO: fix this header
    { name: 'target_classification' },
    { name: 'base_year' },
    { name: 'target_year' },
    { name: 'year_type' },
    { name: 'date_published' },
  ],
  commitment: [
    { name: 'company_name' },
    { name: 'action' },
    { name: 'commitment_type' },
    { name: 'commitment_deadline' },
    { name: 'status' },
    { name: 'reason_for_commitment_extension_or_removal' },
    { name: 'year_type' },
    { name: 'date_published' },
  ]
};

// Check for missing headers
export function checkHeaders(requiredHeaders: TableHeader[], headers: string[]): string[] {
  const missingHeaders = requiredHeaders.filter(header => !headers.includes(header.name));
  return missingHeaders.map(header => header.name);
}