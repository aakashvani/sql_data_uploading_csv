import csv
import psycopg2
from psycopg2 import sql
from datetime import datetime
import tempfile
import shutil

# Database connection details
# Database connection details
DB_NAME = "brookhaven_heart"
DB_USER = "Pawan"
DB_PASSWORD = "Secure@2323$"
DB_HOST = "scale-health-postgres.postgres.database.azure.com"
DB_PORT = 5432

# CSV file path
CSV_FILE_PATH = r"C:\Users\InAakash155\Desktop\BHH - csv\BHH Data - 2023 - Financial analysis.csv"

# Function to convert date strings to date objects
def parse_date(date_str):
    if not date_str:
        return None
    try:
        # str_date = datetime.strptime(date_str, "%m/%d/%Y").date()
        return datetime.strptime(date_str, "%m/%d/%Y").date()
    except ValueError:
        return ''

def upload_csv_to_db():
    try:
        # Connect to the database
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = conn.cursor()

        # Create a temporary file to store updated CSV data
        with tempfile.NamedTemporaryFile(mode='w', newline='', delete=False) as temp_csvfile:
            csv_reader = csv.reader(open(CSV_FILE_PATH, newline=''))
            csv_writer = csv.writer(temp_csvfile)

            header = next(csv_reader)  # Read the header row
            if 'status' not in header:
                header.append('status')  # Add a status column if it doesn't exist
            csv_writer.writerow(header)

            for row in csv_reader:
                try:
                    # Map CSV row to table columns
                    # new_dat = parse_date(row[7])
                    data = (
                        row[0], row[1], row[2], row[3], parse_date(row[4]), parse_date(row[5]),
                        parse_date(row[6]), parse_date(row[7]), int(row[8]), int(row[9]),
                        parse_date(row[10]), row[11], row[12], row[13], row[14].lower() == 'true',
                        row[15], row[16], row[17], row[18], row[19], row[20], row[21], int(row[22]),
                        row[23], row[24], row[25], row[26], row[27], parse_date(row[28]), int(row[29]),
                        row[30], row[31], row[32], row[33], row[34], row[35], row[36], row[37],
                        row[38], row[39], row[40], row[41], row[42], row[43], row[44], row[45],
                        row[46], row[47], row[48], row[49], row[50], row[51], row[52], row[53],
                        row[54], row[55], row[56], row[57], row[58], row[59], row[60], row[61],
                        row[62], row[63], row[64], row[65], row[66], row[67],row[68], row[67], 
                        row[68], row[69], row[70], row[71],row[72], row[73], row[74]
                    )

                    # Insert data into the database
                    insert_query = sql.SQL("""
                        INSERT INTO brookhaven_heart.financial_analysis_cpt.financial_analysis_cpt_report (
                            client, 
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
                            charge_allowed_fee, payment_allowed, billed_units
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,)
                    """)
                    cursor.execute(insert_query, data)
                    conn.commit()
                    row.append('success')
                except Exception as e:
                    conn.rollback()
                    print(f"Error: {e}")
                    row.append('error')

                csv_writer.writerow(row)  # Write the row with status to the temp file

        cursor.close()
        conn.close()

        # Replace the original CSV file with the updated temp file
        shutil.move(temp_csvfile.name, CSV_FILE_PATH)
        print("CSV data processed and status updated successfully")

    except Exception as e:
        print(f"Database connection error: {e}")

if __name__ == "__main__":
    upload_csv_to_db()
