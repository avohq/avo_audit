select *
from (

    select
        event_name
    from {{ ref('experiment_test_data_normal') }}
    GROUP BY event_name
    having count(*) >= 1

) spike_or_drop_errors