# Avo Audit

Avo audit helps you detect issues in your raw analytics event tables by querying a sample of the raw events and comparing them against each other.
This allows you to find discrepancies between events and properties that should look the same, but don’t.

## Issue types

### Issues available in the alpha version:  

**Property sometimes missing** – detects absence of event properties across tracking calls.  
>_Property onboarding status is not sent with 30% of the App Opened event._ 

**Property type mismatch** – detects inconsistent event property types across tracking calls.  
>_Inconsistent type of property onboarding status on event App Opened: int (84%), string (16%)._

### Issue types on the roadmap (not prioritized list):
* Event missing on some platforms
* Property missing on some platforms
* Event volume change significant between versions
* Event volume significantly different between platforms
* Inconsistent event name casing
* Inconsistent property name casing
* Global property type mismatch
* Similar event names
* Similar property names within event
* Unexpected type based on property name
* Global similar property names
* Missing property based on property group pattern


## How do I get the most out of this package? 
We recommend using the [audit](#audit) macro. That way you will generate a SQL table listing the issues found in your project.

## Installing the package
Include the following in your `packages.yml` file in your dbt project:

```
packages:
    - package: avo/avo_audit
      version: ["0.1.0]
```

Run `dbt deps` in the root of your dbt project to install the package.


# Macros

### audit_event_volume
This macro is intended to compare a recent time period by comparing the volume for each day in the time period, to detect significant drops or rises in event volumes.

To execute, run the following commands in your dbt dev environment:"
```
{% set raw_event_relation=adapter.get_relation(
      database=target.database,
      schema="raw_event_schema",
      identifier="raw_event_table"
) -%}

{{ avo_audit.audit_event_volume(
    relation=raw_event_relation,
    end_date="2021-10-25",
    days_back=10,
    days_lag=5,
    event_name_column="event",
    event_date_column="ts",
    event_source_column="client"
) }}
```






## join_schema_to_table
This macro is a helper macro for those who do not have all raw events in a single table, but instead in multiple tables in one dataset.
It is intended to extract the key columns needed and merge all the tables into 1.
This is something we use to be able to use `audit_event_volume`
```
// schema/bigquery dataset
{{
    join_schema_into_table(
        schema="avo_analytics_playground",
        event_name_column="event", 
        event_version_column="version", event_date_column="sent_at", 
        event_source_column="client"
    )
}}
```

### Disclaimer
> This project is still work in progress. These macros have not been fully implemented yet, however some of the work has been started and we will be doing our best to get this up and running as soon as possible. Reach out if you want to contribute to the project or join our alpha group to be one of the first to try out what we build and provide feedback!"



# Macros coming soon.
## audit

Audit runs on your raw events table and generates a table of issue types like the ones listed in [Issue Types](#Issue-types).

This is a macro which joins together 3 internal macros to fully automate the audit process. It runs the following macros in the order listed.

    1. `avo_audit.parse_signature`
    2. `avo_audit.compare_events`
    3. `avo_audit.report_issues`

This macro is only intended join together these 3 steps into 1 dbt model.

To execute, run the following command in your environment:
```
{# in dbt Develop #}

{% set raw_event_relation=adapter.get_relation(
      database=target.database,
      schema="raw_event_schema",
      identifier="raw_event_table"
) -%}

{{ avo_audit.audit(
    relation=raw_event_relation,
    date_partition=”sent_at”,
    lookback_hours=24,
) }}
```
[See Report Issues for documentation on the result model](#report_issues)


## parse_signature

This macro generates a SQL that transforms each raw event into it’s signature.  
Instead of using the signature from the column definitions, it is deriving the signature from the value. This can be used to compare events with identical names together to understand if and how they vary from each other.


**Raw Events:**
| event_name  | user_id      | user_name | device  |
|-------------|--------------|-----------|---------|
| App Opened  | 2c3416a3033e | John      | IOS     |
| App Opened  | fc3416basdq1 | Donna     | IOS     |
| App Opened  | c5e6d9429f27 | Lisa      |         |
| App Opened  | 55331212412  | Joshua    | Android |



**After running parse_signature:**
| event_name | property_name_mapping        | property_signature_mapping |
|------------|------------------------------|----------------------------|
| App Opened | [user_id, user_name, device] | [string, string, string]   |
| App Opened | [user_id, user_name, device] | [string, string, string]   |
| App Opened | [user_id, user_name, device] | [string, string, null]     |
| App Opened | [user_id, user_name, device] | [big int, string, null]        |


We recommend to test this in your development environment first while you get set up.
```
{# in dbt Develop #}

{% set raw_event_relation=adapter.get_relation(
      database=target.database,
      schema="raw_event_schema",
      identifier="raw_event_table"
) -%}

{{ avo_audit.parse_signature(
    relation=raw_event_relation,
    date_partition=”sent_at”,
    lookback_hours=1,
) }}
```

**Arguments:**
* **raw_event_relation:** The database table containing your raw events which you want to compare.
* **date_partition:** The column in raw events that can be used to filter on time to limit query.
* **lookback_hours:** How many hours the query should look back, we recommend keeping it under 24h to keep the cost of the query low.



## compare_events

This macro will run on the [parse_signature](#parse_signature) result table and group together the events based on event_name, property_name_mapping and property_signature_mapping to count how many times the event varies and what is the most common variation being sent.

The results will look like this:

| event_name | property_name_mapping        | property_signature_mapping | variant_count | total_event_count | number_of_event_variants |
|------------|------------------------------|----------------------------|---------------|-------------------|--------------------------|
| App Opened | [user_id, user_name, device] | [string, string, string]   | 2             | 4                 | 3                        |
| App Opened | [user_id, user_name, device] | [string, string, null]     | 1             | 4                 | 3                        |
| App Opened | [user_id, user_name, device] | [big int, string, null]        | 1             | 4                 | 3                        |


To execute, run the following commands in your dbt dev environment:"
```
{# in dbt Develop #}

{% set raw_event_signature_relation=adapter.get_relation(
      database=target.database,
      schema="avo_audit_schema",
      identifier="raw_event_signature_table"
) -%}

{{ avo_audit.compare_events(
    relation=raw_event_signature_relation
) }}
```

## report_issues

This macro takes the result from [compare_events](#compare_events), and generates issues based on number of event variants, which kind of variant difference it is etc. The issues are priorities in the following order.

* **Type difference** - Inconsistent type of property onboarding status on event App Opened: int (84%), string (16%).
* **Wrong property name casing** - property name is not following the general casing of the project.
* **Property Sometimes missing** -  Property onboarding status is not sent with 30% of the App Opened event.
* **Event missing on some platforms** - Event "App Opened" has never been seen on platform "Web"
* **Property missing on some platforms** - Property "Device" on Event "App Opened" missing on Platform "Web"
* **Event volume change significant between versions** - Event "App Opened" has dropped (Down 31%) to "10123" on version 1.2.3, from 14834 on version "1.2.2" 
* **Event volume difference significant between platforms** - Event "App Open" is being sent 3374(33%) on Android but 6748 (67%) on IOS
* **Incosistent Event name casing** - Event Name "App Opened" is not following the general casing of the project.
* **Global property type mismatch** - Property "Device" is sent as string on "App Opened Event" but as int on "Signed Up" event.
* **Similar event names** - Event name "App Opened" is similar to event name "app_opened"
* **Similar property names within event** - Property name User Id is similiar to property name user_id
* **Global similar property names** - Property name User Id is similiar to property name user_id
* **Missing property based on property group pattern** - Property "Device" is missing on Event "App Closed", grouped with "user_id","user_name" properties on event "App Opened"

| event_name | property_name_mapping        | property_signature_mapping | variant_count | total_event_count | number_of_event_variants | issue_priority | issue                                                                                 |
|------------|------------------------------|----------------------------|---------------|-------------------|--------------------------|----------------|---------------------------------------------------------------------------------------|
| App Opened | [user_id, user_name, device] | [big int, string, null]        | 1             | 4                 | 3                        | 1              | Inconsistent type of property "user_id" on event App Opened: big int (25%), string (75%). |
| App Opened | [user_id, user_name, device] | [string, string, null]     | 1             | 4                 | 3                        | 4              | Property "device" is not sent with 25% of the App Opened event.                       |


Execute the following commands in your dev environment
```
{# in dbt Develop #}

{% set compare_event_relation=adapter.get_relation(
      database=target.database,
      schema="avo_audit_schema",
      identifier="compare_events_table"
) -%}

{{ avo_audit.report_issues(
    relation=compare_events_relation,
    report_missing_property=true
) }}
```

**Arguments:**
* **relation:** The database table containing your raw events which you want to compare.
* **report_missing_property:** Whether the report should include property missing, defaults to true


# Upcoming Macros

### Disclaimer
> Here we list things that we plan on building. If you want to contribute, this list of macros is up for grabs!



# View the issues report
 
The issues table can be large and overwhelming, with multiple issues each with different priority. We will build the **avo_audit_cli** python library to better visualize the issues.

In **avo_audit_cli** you can either
* Review and filter issues visually with the `avo_audit_cli serve` serve command which generates a local html file.
* Enable the inspector dashboard and Tracking plan implementation status in your Avo workspace with `avo_audit_cli post --token` which posts the results to the avo workspace

**avo_audit_cli has not yet been implemented but will be coming soon!**
