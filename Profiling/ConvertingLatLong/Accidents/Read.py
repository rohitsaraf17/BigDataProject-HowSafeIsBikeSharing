import csv
from uszipcode import SearchEngine, SimpleZipcode, Zipcode
search = SearchEngine()
with open('outputPostCode.csv', 'w') as f:
    #writer =csv.writer(f)
    with open('outputCleanData.csv', 'r') as csvFile:
        reader = csv.reader(csvFile)
        for row in reader:
            newstr1 = row[2].replace("'", "").strip()
            newstr2 = row[3].replace("'", "").strip()
            if newstr1=="" or newstr2 =="":
                continue
            result = search.by_coordinates(float(newstr1), float(newstr2), radius=30, returns=1)
            for zipcode in result:
                print(newstr1, "and", newstr2, "and", zipcode.zipcode)
                row[4]="'"+str(zipcode.zipcode)+"'"
                str1 = ','.join(row)
                f.write(str1+"\n")
    csvFile.close()
f.close()