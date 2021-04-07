# Sync Demogrpahic Data from EveryAction

EveryAction does not include race and gender in their commercial Pipeline sync. Sunrise engineered a custom sync to bring in demographic data to our Redshift warehouse. Our sync brings in gender, race, as well as some other custom fields. You can modify this script to bring in any custom fields you desire. 

## Requirements
This project assumes that:

* You're using EveryAction
* You're using Redshift
* You have a Parsons VAN API key

## Notes

[Get Extra Fields Container](https://github.com/sunrisedatadept/sync-everyaction-demographic-data/blob/main/get_extra_fields_container.py)  
This is the container version of the script. This is what you want to point your Civic Container script to. It will not run locally. 

## Usage

1. Clone this Github repository -- you'll need to specify your new url in the civis interface
2. Create a new Container Script in Civis
3. The following parameters must be set in the script for this to work:

| PARAMETER NAME | DISPLAY NAME | DEFAULT | TYPE              | MAKE REQUIRED |
|----------------|--------------|---------|-------------------|---------------|
| VAN            | VAN          | N/A     | Custom Credential | Yes           |
| REDSHIFT       | REDSHIFT     | N/A     | Database          | Yes           |
| AWS            | AWS          | N/A     | AWS Credential    | Yes           |

4. Connect civis to your github repository and point it appropriately.

5. Put the following in the command lines COMMAND section:

```
pip install pandas
python get_extra_fields_container.py

```


