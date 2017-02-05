## Japanese Trade Analysis using Hadoop and Map-Reduce Java

In this project, Japanese import-export trade was analyzed with various countries across the globe using 28 years of historic trade data and future prediction is made for the top trade countries.It was successfully tested on 4 Node Hadoop Cluster on AWS.

### Dataset Link: https://www.kaggle.com/zanjibar/japan-trade-statistics

###Instructions to run the code:
===============================

1. Compiling code using any of below two methods: 
   1.1 Compile the code into jar file and copy input files to hdfs 
   1.2 Create a Maven project and export it to jar.You can also do this using Eclipse

2. Run the compiled jar file using the following command 
Hadoop jar application.jar App <input files locations> <output files location>

3. Multiple folders will be created under output

4. Step1 folder is the output after first map reduce job which will have saperate aggregated trade data for all years one file for each country 

5. Step 2 folder contains the predictions trade amounts for all countries for year 2016

6. Final folder contains sorted list of predictions which helps to pick top layer countries . We have picked top 5 and projected with visualization in ppt
