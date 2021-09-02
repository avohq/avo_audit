# Avo Audit

Avo audit helps you detect issues in your raw analytics event tables by querying a sample of the raw events and comparing them against each other.
This allows you to find discrepancies between events and properties that should look the same, but don’t.

## Issue types

Issues available in the alpha version:  

**Property sometimes missing** – detects absence of event properties across tracking calls.  
>`Property onboarding status is not sent with 30% of the App Opened event.`  

**Property type mismatch** – detects inconsistent event property types across tracking calls.  
>`Inconsistent type of property onboarding status on event App Opened: int (84%), string (16%).`

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
We recommend implementing at least the main 3 macros, and chain them together. That way you will generate a SQL table listing the issues found in your project, ordered by the priority listed above.

Steps:
1. Convert raw events into it’s signature type with parse_signature
2. Group events by identical signatures and compare events with different signatures Per event with compare_events
3. Generate issues table by using the report_issues macro.


## Installing the package
Include the following in your `packages.yml` file in your dbt project:

```
packages:
    - package: avo/avo_audit
      version: ["0.1.0]
```

Run `dbt deps` in the root of your dbt project to install the package.


# Macros

### parse_signature

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
| App Opened | [user_id, user_name, device] | [int, string, null]        |


It is best to test this in your develop environment first while you get set up.
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



### compare_events

This macro will run on the results of parse_signature.

It will execute a Sql that will go through each event_name group, and compare it together with all the other events to find issues where a field is sometimes NULL, Int instead of string etc.

The end result will be a table that identifies discrepancies between the events.

| event_name | property_name_mapping        | property_signature_mapping | variant_count | total_event_count | number_of_event_variants |
|------------|------------------------------|----------------------------|---------------|-------------------|--------------------------|
| App Opened | [user_id, user_name, device] | [string, string, string]   | 2             | 4                 | 3                        |
| App Opened | [user_id, user_name, device] | [string, string, null]     | 1             | 4                 | 3                        |
| App Opened | [user_id, user_name, device] | [int, string, null]        | 1             | 4                 | 3                        |


```
{# in dbt Develop #}

{% set raw_event_signature_relation=adapter.get_relation(
      database=target.database,
      schema="avo_audit_schema",
      identifier="raw_event_signature_table"
) -%}

{{ avo_audit.parse_signature(
    relation=raw_event_signature_relation
) }}
```

### report_issues

This macro takes the result from compare_events, and generates issues based on number of event variants, which kind of variant difference it is etc. The issues are priorities in the following order.

* **Type difference** - Inconsistent type of property onboarding status on event App Opened: int (84%), string (16%).
* **Wrong casing** - property name is not following the general casing of the project.
* **Property Sometimes missing** -  Property onboarding status is not sent with 30% of the App Opened event.

| event_name | property_name_mapping        | property_signature_mapping | variant_count | total_event_count | number_of_event_variants | issue_priority | issue                                                                                 |
|------------|------------------------------|----------------------------|---------------|-------------------|--------------------------|----------------|---------------------------------------------------------------------------------------|
| App Opened | [user_id, user_name, device] | [int, string, null]        | 1             | 4                 | 3                        | 1              | Inconsistent type of property "user_id" on event App Opened: int (25%), string (75%). |
| App Opened | [user_id, user_name, device] | [string, string, null]     | 1             | 4                 | 3                        | 4              | Property "device" is not sent with 25% of the App Opened event.                       |


**Arguments:**
* **compare_event_relation:** The database table containing your raw events which you want to compare.
* **report_missing_property:** Whether the report should include property missing, defaults to true


## Upcoming Macros

### audit_volume
This macro is intended to compare 2 days of data to each other, and compare for each event relative to number of events received to identify if any event or event signature significantly drops or rises in volume.

Code to use the macro:

```
{% set raw_event_relation=adapter.get_relation(
      database=target.database,
      schema="raw_event_schema",
      identifier="raw_event_table"
) -%}

{{ avo_audit.audit_volume(
    relation=raw_event_relation,
    date_partition=”sent_at”,
    first_date: <Datetime>,
    Second_date: <Datetime>
) }}
```


# View the issues report
 
The issues table can be large and overwhelming, with multiple issues each with different priority. We will build the **avo_audit_cli** python library to better visualize the issues.

**In avo_audit_cli you can either**
* Review and filter issues visually with the `avo_audit_cli serve` serve command which generates a local html file.
* Enable the inspector dashboard and Tracking plan implementation status in your Avo workspace with `avo_audit_cli post --token` which posts the results to the avo workspace

**avo_audit_cli has not yet been implemented but will be coming soon!**
