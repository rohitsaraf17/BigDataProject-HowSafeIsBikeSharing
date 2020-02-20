cat urls.txt | xargs -n 1 -P 6 wget -P data/
unzip 'data/*.zip' -d data/
rm -r /home/mn2643/FinalProject/data*.zip
