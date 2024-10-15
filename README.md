# BCG Case Study : Car Crash Data 

This project is a PySpark-based data pipeline designed to process large datasets and generate insights through various transformations. The pipeline is controlled via the command line and is suitable for batch processing in distributed environments.

## Table of Contents

- [Project Description](#bcg-case-study--car-crash-data)
- [Problem Statement](#problem-statement)
- [Installation](#installation)
- [Usage](#usage)
- [Project Architecture](#project-architecture)
- [Data](#data)
- [Configuration](#configuration)
- [License](#license)

## Problem Statement
- A sample database of vehicle accidents across US for brief amount of time is given.
- Develop an application which can perform following analysis on it and store its results -
    1. Analysis 1: 
        - Find the number of crashes (accidents) in which number of males killed are greater than 2?
    2. Analysis 2: 
        - How many two wheelers are booked for crashes?
    3. Analysis 3: 
        - Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.
    4. Analysis 4: 
        - Determine number of Vehicles with driver having valid licences involved in hit and run?
    5. Analysis 5: 
        - Which state has highest number of accidents in which females are not involved?
    6. Analysis 6: 
        - Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
    7. Analysis 7: 
        - For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
    8. Analysis 8: 
        - Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
    9. Analysis 9: 
        - Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
    10. Analysis 10: 
        - Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)

## Installation

1. **Clone the repository**:

    ```bash
    git clone https://github.com/ameyss97/CarCrash_CaseStudy_Analytics.git
    cd carCrash
    ```
2. **Install dependencies** : Ensure you have Python installed and install the required packages using pip:
    ```
    pip install -r requirements.txt
    ```

## Usage

For production deployments and distributed setup, `spark-submit` is definitely recommended.:

Basic Command:
```bash
spark-submit \
--master local[4] \
--conf "spark.executor.memory=4g" \
--conf "spark.executor.cores=2" \
main.py
```

You can fine tune Spark Cluster as usual via Spark-submit:
[Refer Spark-Submit Docs](https://spark.apache.org/docs/latest/submitting-applications.html)
```bash
spark-submit \
--master local[4] \
--conf "spark.executor.memory=4g" \
--conf "spark.executor.cores=2" \
main.py
```

## Project Architecture

### References

1. [GitHub Repo | data science project folder structure)
](https://github.com/hardefarogonondo/data-science-project-folder-structure)

2. [Medium Article | GitHub Repository Structure Best Practices](https://medium.com/code-factory-berlin/github-repository-structure-best-practices-248e6effc405)

### Folder Structure
```plaintext
.
├── config/                  # Contains config files
│   └── path_config.yaml          # Input and output paths
├── data/                    # Data storage
│   ├── input/               # Raw input data as CSV files
│   │   ├── Charges_use.csv          
│   │   ├── Damages_use.csv         
│   │   ├── Endorse_use.csv           
│   │   ├── Primary_Person_use.csv        
│   │   ├── Restrict_use.csv          
│   │   └── Units_use.csv          
│   └── output/              # Final output files to store analysis results
│       ├── Output_one.csv          
│       ├── Output_two.csv         
│       ├── Output_three.csv           
│       ├── Output_four.csv        
│       ├── Output_five.csv          
│       ├── Output_six.csv          
│       ├── Output_seven.csv          
│       ├── Output_eight.csv          
│       ├── Output_nine.csv          
│       └── Output_ten.csv           
├── jobs/                    # PySpark job scripts
│   └── analyser.py          # Job script where actual analysis code is present
├── notebooks/               # PySpark notebooks for different tasks
│   ├── appdev1.ipynb        # Notebook to try out different development ideas
│   └── appdev2.ipynb        # Notebook to try out analysis code
├── utils/                   # Utility functions or helper scripts
│   ├── config_loader.py     # utility for loading configurations
│   ├── data_loader.py       # utility for loading input data
│   └── output_writer.py     # utility for writing output data
├── .gitignore               # Git ignore file 
├── LICENSE                  # MIT Open Source License
├── main.py                  # Entry point for running the project 
├── README.md                # Project description and instructions
└── requirements.txt         # Python dependencies
```

## Data

- **Input Folder**: 
    - Contains 6 CSV files that are used as input data
    - Charges
        - CRASH_ID : Crash ID – System-generated unique identifying number for a crash
        - UNIT_NBR : The unit of the person who was charged
        - PRSN_NBR : The person who was charged
        - CHARGE : Charge
        - CITATION_NBR : Citation/Reference Number
    - Damages
        - CRASH_ID : Crash ID – System-generated unique identifying number for a crash
        - DAMAGED_PROPERTY : Damaged Property other than Vehicles
    - Endorsements
        - CRASH_ID : Crash ID – System-generated unique identifying number for a crash
        - UNIT_NBR : The unit of the person who was charged
        - DRVR_LIC_ENDORS_ID : Commercial Driver License Endorsements
    - Primary Person
        - CRASH_ID : Crash ID – System-generated unique identifying number for a crash  
        - UNIT_NBR : Unit number entered on crash report for a unit involved in the crash
        - PRSN_NBR : Person number captured on the crash report
        - PRSN_TYPE_ID : Person Type
        - PRSN_OCCPNT_POS_ID : The physical location of an occupant in, on, or outside of the motor vehicle prior to the First Harmful Event or loss of control
        - PRSN_INJRY_SEV_ID : Severity of injury to the occupant
        - PRSN_AGE : Age of person involved in the crash
                Ethnicity of person involved in the crash
        - PRSN_GNDR_ID : Gender of person involved in the crash
        - PRSN_EJCT_ID : The extent to which the person's body was expelled from the vehicle during any part of the crash
        - PRSN_REST_ID : The type of restraint used by each occupant
        - PRSN_AIRBAG_ID : Indicates whether a person's airbag deployed during the crash and in what manner
        - PRSN_HELMET_ID : Indicates if a helmet was worn at the time of the crash
        - PRSN_SOL_FL : Solicitation flag
        - PRSN_ALC_SPEC_TYPE_ID : Type of alcohol specimen taken for analysis from the primary persons involved in the crash
        - PRSN_ALC_RSLT_ID : Alcohol Result
        - PRSN_BAC_TEST_RSLT : Numeric blood alcohol content test result for a primary person involved in the crash, using standardized alcohol breath results (i.e. .08 or .129)
        - PRSN_DRG_SPEC_TYPE_ID : Type of drug specimen taken for analysis from the primary persons involved in the crash
        - PRSN_DRG_RSLT_ID : Primary person drug test result
        - DRVR_DRG_CAT_1_ID : First category of drugs related to the driver
        - PRSN_DEATH_TIME : Primary person's time of death.
        - INCAP_INJRY_CNT : Suspected Serious Injury Count
        - NONINCAP_INJRY_CNT : Non-incapacitating Injury Count
        - POSS_INJRY_CNT : Possible Injury Count
        - NON_INJRY_CNT : Not Injured Count
        - UNKN_INJRY_CNT : Unknown Injury Count
        - TOT_INJRY_CNT : Total Injury Count
        - DEATH_CNT : Death Count
        - DRVR_LIC_TYPE_ID : Driver's license type
        - DRVR_LIC_STATE_ID : The state or province that issued the vehicle driver's license or identification card
        - DRVR_LIC_CLS_ID : Driver's license clas
        - DRVR_ZIP : Driver's address - zip code
    - Restrict
        - CRASH_ID : Crash ID – System-generated unique identifying number for a crash
        - UNIT_NBR : The unit of the person who was charged
        - DRVR_LIC_RESTRIC_ID : Driver License Restrictions
    - Unit
        - CRASH_ID : Crash ID – System-generated unique identifying number for a crash
        - UNIT_NBR : Unit number entered on crash report for a unit involved in the crash
        - UNIT_DESC_ID : Unit Description  - Describes the type of unit
        - VEH_PARKED_FL : Parked Vehicle
        - VEH_HNR_FL : Hit and Run
        - VEH_LIC_STATE_ID : License Plate State
        - VIN : VIN
        - VEH_MOD_YEAR : 4–digit numeric model year of the vehicle as designated by the manufacturer
        - VEH_COLOR_ID : Vehicle Color
        - VEH_MAKE_ID : The vehicle manufacturer’s distinctive name applied to a group of motor vehicles (Ford, Chevrolet, etc.)
        - VEH_MOD_ID : The vehicle manufacturer’s trade name
        - VEH_BODY_STYL_ID : The body style of the vehicle involved in the crash
        - EMER_RESPNDR_FZ : Indicates if a peace officer, firefighter, or emergency medical services employee is involved in a crash while driving a law enforcement vehicle, fire department vehicle, or medical emergency services vehicle while on emergency
        - OWNR_ZIP : Owner Address, Zip
        - FIN_RESP_PROOF_ID : Indicates whether the vehicle driver presented satisfactory evidence of financial responsibility
        - FIN_RESP_TYPE_ID : Financial Responsibility Type
        - VEH_DMAG_AREA_1_ID : Vehicle Damage Rating 1 - Area
        - VEH_DMAG_SCL_1_ID : Vehicle Damage Rating 1 - Severity
        - FORCE_DIR_1_ID : Vehicle Damage Rating 1 - Direction of Force
        - VEH_DMAG_AREA_2_ID : Vehicle Damage Rating 2 - Area
        - VEH_DMAG_SCL_2_ID : Vehicle Damage Rating 2 - Severity
        - FORCE_DIR_2_ID : Vehicle Damage Rating 1 - Direction of Force
        - VEH_INVENTORIED_FL : Vehicle Inventoried
        - VEH_TRANSP_NAME : Towed By
        - VEH_TRANSP_DEST : Towed To
        - CONTRIB_FACTR_1_ID : The first factor for the vehicle  which the officer felt contributed to the crash
        - CONTRIB_FACTR_2_ID : The second factor for the vehicle  which the officer felt contributed to the crash
        - CONTRIB_FACTR_P1_ID : The first factor for the vehicle  which the officer felt contributed to the crash
        - VEH_TRVL_DIR_ID : Cardinal direction that the vehicle was traveling prior to the First Harmful Event or loss of control
        - FIRST_HARM_EVT_INV_ID : IF= Indicate First Harmful Event Involvement
        - INCAP_INJRY_CNT : Suspected Serious Injury Count
        - NONINCAP_INJRY_CNT : Non-incapacitating Injury Count
        - POSS_INJRY_CNT : Possible Injury Count
        - NON_INJRY_CNT : Not Injured Count
        - UNKN_INJRY_CNT : Unknown Injury Count
        - TOT_INJRY_CNT : Total Injury Count
        - DEATH_CNT : Death Count
        
- **Output Folder**: 

    - Contains 10 result files of analysis
    - Output_one
    - Output_two
    - Output_three
    - Output_four
    - Output_five
    - Output_six
    - Output_seven
    - Output_eight
    - Output_nine
    - Output_ten

## Configuration

- **Path Configuration**: 
    - Provide Input Output paths for CSVs in `config/path_config.json`. 


## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.