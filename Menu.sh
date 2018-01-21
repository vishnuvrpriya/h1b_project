#!/bin/bash 
show_menu()
{
    NORMAL=`echo "\033[m"`
    MENU=`echo "\033[36m"` #Blue                              
    NUMBER=`echo "\033[33m"` #yellow
    FGRED=`echo "\033[41m"`
    RED_TEXT=`echo "\033[31m"`
    ENTER_LINE=`echo "\033[33m"`
        echo -e "${MENU}**********************ANALYSIS AND SUMMERIZATION OF H1B APPLICANTS***********************${NORMAL}"
    echo -e "${MENU}${NUMBER} 1A) ${MENU} Is the number of petitions with Data Engineer job title increasing over time?(MR)${NORMAL}"
    echo -e "${MENU}${NUMBER} 1B) ${MENU} Find top 5 job titles who are having highest growth in applications.(MR) ${NORMAL}"
    echo -e "${MENU}${NUMBER} 2A) ${MENU} Which part of the US has the most Data Engineer jobs for each year?(HIVE)(MR) ${NORMAL}"
    echo -e "${MENU}${NUMBER} 2B) ${MENU} Find top 5 locations in the US who have got certified visa for each year.(HIVE)(MR)${NORMAL}"
    echo -e "${MENU}${NUMBER} 3) ${MENU} Which industry has the most number of Data Scientist positions?(MR)${NORMAL}"
    echo -e "${MENU}${NUMBER} 4) ${MENU} Which top 5 employers file the most petitions each year?(MR) ${NORMAL}"
    echo -e "${MENU}${NUMBER} 5A) ${MENU} Find the most popular top 10 job positions for H1B visa applications for each year?(HIVE)(MR)${NORMAL}"
    echo -e "${MENU}${NUMBER} 5B) ${MENU} Find the most popular top 10 job positions for Certified H1B visa applications for each year?(HIVE)(MR)${NORMAL}"
    echo -e "${MENU}${NUMBER} 6) ${MENU} Find the percentage and the count of each case status on total applications for each year. Create a graph depicting the pattern of All the cases over the period of time.(PIG)(MR)${NORMAL}"
    echo -e "${MENU}${NUMBER} 7) ${MENU} Create a bar graph to depict the number of applications for each year(PIG)(MR)${NORMAL}"
    echo -e "${MENU}${NUMBER} 8) ${MENU}Find the average Prevailing Wage for each Job for each Year (take part time and full time separate) arrange output in descending order(MR)${NORMAL}"
    echo -e "${MENU}${NUMBER} 9) ${MENU}Which are the employers who have highest success rate in petitions more than 70%and total petions filed more than 1000?(PIG)(MR)${NORMAL}"
    echo -e "${MENU}${NUMBER} 10) ${MENU}Which are the top 10 job positions which have the  success rate more than 70% in petitions and total petitions filed more than 1000? (PIG)(MR)${NORMAL}"
    echo -e "${MENU}${NUMBER} 11) ${MENU}Export result for option no 12 to MySQL database.(Sqoop,Hive)${NORMAL}"
    echo -e "${MENU}*********************************************${NORMAL}"
    echo -e "${ENTER_LINE}Please enter a menu option and enter or ${RED_TEXT}enter to exit. ${NORMAL}"
    read opt
}
function option_picked() 
{
    COLOR='\033[01;31m' # bold red
    RESET='\033[00;00m' # normal white
    MESSAGE="$1"  #modified to post the correct option selected
    echo -e "${COLOR}${MESSAGE}${RESET}"
}



clear
show_menu
while [ opt != '' ]
    do
    if [[ $opt = "" ]]; then 
            exit;
    else
        case $opt in
        1A) clear;
        option_picked "1A) Is the number of petitions with Data Engineer job title increasing over time?";
                 hadoop fs -rm -r /H1B_Project/MapReduce/Output/Q1ADataEngg
		 hadoop jar DataJob.jar   DataJobFinal /h1bmp/h1bfinaldata.txt /H1B_Project/MapReduce/Output/Q1ADataEngg
		 hadoop fs -cat /H1B_Project/MapReduce/Output/Q1ADataEngg/p*
        show_menu;
        ;;

	1B) clear;
        option_picked "1B) Find top 5 job titles who are having highest growth in applications. ";
		    hive -f Question1B.sql     
        show_menu;
        ;;

	2A) clear;
        option_picked "2A) Which part of the US has the most Data Engineer jobs for each year?";
	 hadoop fs -rm -r /H1B_Project/MapReduce/Output/Q2AUSParts
		 hadoop jar DataJob.jar USPartsFinal /h1bmp/h1bfinaldata.txt /H1B_Project/MapReduce/Output/Q2AUSParts
		 hadoop fs -cat /H1B_Project/MapReduce/Output/Q2AUSParts/p*
        show_menu;	
        ;;

	2B) clear;
        option_picked "2B) Find top 5 locations in the US who have got certified visa for each year.";
        echo -e "Enter the year (2011,2012,2013,2014,2015,2016)"
		read var
	    hive -e "select count(*) as apps,worksite,year from h1b_final where case_status = 'CERTIFIED' and year = '$var' group by worksite,year order by apps DESC LIMIT 5;" 
        show_menu;
        ;;

	3) clear;
        option_picked "3) Which industry has the most number of Data Scientist positions?";
	 
         hive -e  "select count(job_title) as jobapps,soc_name from h1b_final where job_title = 'DATA SCIENTIST' and case_status = 'CERTIFIED' group by soc_name order by jobapps desc limit 1;"

        show_menu;
        ;;

	4) clear;
        option_picked "4)Which top 5 employers file the most petitions each year?";
                 echo -e "Enter the year (2011,2012,2013,2014,2015,2016,all)"
                    read var
                  hadoop fs -rm -r /H1B_Project/MapReduce/Output/Q4Top5Employers
		 hadoop jar Top5EmployersFinal.jar Top5Employers /h1bmp/h1bfinaldata.txt /H1B_Project/MapReduce/Output/Q4Top5Employers
		 hadoop fs -cat /H1B_Project/MapReduce/Output/Q4Top5Employers/p*
        show_menu;
        ;;
           
	5A) clear;
        option_picked "5A) Find the most popular top 10 job positions for H1B visa applications for each year?";
	    pig -x local /home/hduser/h1bOPs/Pig/Q5_pig./Q5_10pop_jobpositions_1.pig
        show_menu;
        ;;

        5B) clear;
        option_picked "5B)Find the most popular top 10 job positions for Certified H1B visa applications for each year ?";
	    echo -e "Enter the year (2011,2012,2013,2014,2015,2016)"
		read var
	    hive -e "select count(*) as apps,year,job_title from h1b_final where case_status = 'CERTIFIED' and year = '$var' group by year,job_title order by apps desc limit 10;"
        show_menu;
        ;;
       
	6) clear;
       option_picked "6) Find the percentage and the count of each case status on total applications for each year. Create a graph depicting the pattern of All the cases over the period of time.";
          
              pig -x local /home/hduser/h1bOPs/Pig/Q6_pig./Q6_percent_count.pig
		
        show_menu;
        ;;  

	7) clear;
		
        option_picked "7) Create a bar graph to depict the number of applications for each year";
			
                    hive -f Question7.sql;
		
        show_menu;
        ;;           
        
	8) clear;
        option_picked "8) Find the average Prevailing Wage for each Job for each Year (take part time and full time separate) arrange output in descending order";
		
		echo -e "Enter the case_status (1-Full time position,2-Part time position)";
			read n
			case "$n" in 
			"1")
                         option_picked "Full Time Position" 
                         echo -e "Enter the year (2011,2012,2013,2014,2015,2016,ALL)"
                          read var
			hadoop fs -rm -r  /H1B_Project/MapReduce/Output/Q8AAvgPrWageForEachJobForEachYearFullTime/output
		 hadoop jar AvgWageFTFinal.jar AvgWageFT /h1bmp/h1bfinaldata.txt /H1B_Project/MapReduce/Output/Q8AAvgPrWageForEachJobForEachYearFullTime/output $var;
		 hadoop fs -cat /H1B_Project/MapReduce/Output/Q8AAvgPrWageForEachJobForEachYearFullTime/output/p*
        ;;

			"2")
			  option_picked "Part Time Position" 
                         echo -e "Enter the year (2011,2012,2013,2014,2015,2016,ALL)"
                          read var
                         hadoop fs -rm -r  /H1B_Project/MapReduce/Output/Q8BAvgPrWageForEachJobForEachYearPartTime/Output
                        hadoop jar AvgWagePTFinal.jar AvgWagePT /h1bmp/h1bfinaldata.txt /H1B_Project/MapReduce/Output/Q8AAvgPrWageForEachJobForEachYearFullTime/output $var;
		 hadoop fs -cat /H1B_Project/MapReduce/Output/Q8AAvgPrWageForEachJobForEachYearFullTime/output/p*
        ;;

		esac
		show_menu;
		;;

	9) clear;
		option_picked "9) Which are the employers who have highest success rate in petitions more than 70%and total petions filed more than 1000?"
		 pig -x local /home/hduser/h1bOPs/Pig/Q9_pig./Q9_emp_success_rate.pig
        show_menu;
        ;;

	
	10) clear;
		option_picked "10) Which are the top 10 job positions which have the  success rate more than 70% in petitions and total petitions filed more than 1000?"
		  pig -x local /home/hduser/h1bOPs/Pig/Q10_pig./Q10_job_success_rate.pig
        show_menu;
        ;;
       11) clear;
		option_picked "11) Export Result Of option no 10 to MySQL Database."

                 mysql -u root -p;
                
        show_menu;
        ;;
         
	
        \n) exit;
        ;;

        *) clear;
        option_picked "Pick an option from the menu";
        show_menu;
        ;;
    esac
fi



done
