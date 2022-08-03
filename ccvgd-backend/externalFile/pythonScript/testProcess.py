from Category import CATEGORIES
import process.process_csv_Year_Of_First_Availability_Purchase

read_csv_dirct = "All Datasets and Data Dictionary"
output_csv_dirct = "Database data"



print("---------------")
process.create_csv_category(CATEGORIES, output_csv_dirct)
print("end")


print("---------------")
process.process_csv_Year_Of_First_Availability_Purchase.create_csv_year_of_first_availability_purchase(read_csv_dirct, output_csv_dirct, input_file = "6__year_of_first_availability_purchase.csv")
print("end")