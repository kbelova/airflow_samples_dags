-- create local variable and get value from predefined global variable
{% set my_lovely_variable = yesterday_ds %}

-- create local variable and get value from xcom
{% set var_from_xcom = ti.xcom_pull(key='my_unique_message', task_ids='send_xcom') %}

SELECT
-- define for loop to generate columns
    {% for i in range(1, 5) %}
    -- jinja loop has `index` which allow us to get index of the current element
        {% if loop.index > 1 %}
         ,
        {% endif %}
        -- inside block variables must be used without {}
        {% if i%2==0 %}
            {{i}}  -- outside the block we need {} to have value evaluated.
        {% else %}
            {{i}} || 'is prime'
        {% endif %}
    {% endfor %}
    , 'my_lovely_variable=' || {{ my_lovely_variable }}
    , 'var_from_xcom=' || {{ var_from_xcom }}

;