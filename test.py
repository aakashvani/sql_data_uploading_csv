sample_string = """client, 
                            service_month,
                            claim_month,transaction_month, service_date, claim_date,
                            start_date_of_service, end_date_of_service, unique_visits, unique_claims,
                            payments_date, claim_status_code, claim_status_group_name, visit_type, is_televisit,
                            primary_payer, primary_payer_subscriber_no, secondary_payer, secondary_payer_subscriber_no,
                            tertiary_payer, tertiary_payer_subscriber_no, facility, facility_pos,
                            appointment_servicing_provider, rendering_provider, resource_provider, patient,
                            patient_acct_no, patient_dob, patient_age, patient_gender, patient_race, patient_ethnicity,
                            patient_address_line_1, patient_address_line_2, patient_city, patient_state, patient_zip_code,
                            patient_country, patient_country_code, patient_cell_phone, patient_home_phone, patient_work_phone,
                            claim_no, cpt_code, cpt_description, cpt_group_name, modifier_1, modifier_2, modifier_3, modifier_4,
                            icd1_code, icd1_name, icd2_code, icd2_name, icd3_code, icd3_name, icd4_code, icd4_name, billed_charge,
                            payer_charge, self_charge, total_payment, total_adjustments, balance, payer_payment, patient_payment,
                            contractual_adjustment, payer_withheld, writeoff_adjustment, refund, fee_schedule_allowed_fee,
                            charge_allowed_fee, payment_allowed, billed_units"""


for item in sample_string.split(","):
    print(item)