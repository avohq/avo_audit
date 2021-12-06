select *
from (

    select
        event_name
    from {{ ref('experiment_test_data_significant_drop') }}
    GROUP BY event_name
    having count(*) < 1

) no_data_error