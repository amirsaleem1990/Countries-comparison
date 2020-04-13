rm -rf all_csvs.csv pak-and-egypt-used-and-new.db
for i in $(find . -name *.csv); do cat $i >> all_csvs.csv; done
echo "********************



final csv contains: " ; wc -l all_csvs.csv; echo "



********************"
sed "s/$/,$(date +'%F %T')/" all_csvs.csv > all_csvs-with-date.csv
# rm -rf all_csvs.csv
# mv all_csvs-with-date.csv all_csv.csv
python3 csv-to-db-python-script