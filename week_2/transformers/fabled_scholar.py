import inflection as inf  # This is a Python library for convenient string transformation operations, including converting between different cases (e.g., Camel Case to Snake Case).
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    print("Rows with zero passengers:", data['passenger_count'].isin([0]).sum())
    
    print("Original Column Names:", data.columns)

    snake_case_columns = [inf.underscore(col) for col in data.columns] #Transform column names from Camel Case to Snake Case.
    transformed_data = data.rename(columns=dict(zip(data.columns, snake_case_columns)))

    print("Transformed Column Names:", transformed_data.columns)
   

    original_columns = data.columns
    transformed_columns = transformed_data.columns
    mismatch_count = sum(original != transformed for original, transformed in zip(original_columns, transformed_columns))

    print("Number of mismatched columns:", mismatch_count)

    unique_vendor_ids = transformed_data['vendor_id'].unique()
    print("Уникальные значения в столбце 'vendor_id':", unique_vendor_ids)

    transformed_data['lpep_pickup_date'] = transformed_data['lpep_pickup_datetime'].dt.date


    unique_lpep_pickup_date = transformed_data['lpep_pickup_date'].nunique()

    print("Уникальные значения в столбце 'unique_lpep_pickup_date':", unique_lpep_pickup_date)


    vendor_id_counts = transformed_data['vendor_id'].value_counts()
    print("Распределение уникальных значений в столбце 'vendor_id':")
    print(vendor_id_counts)

    #return data[data['passenger_count'] > 0 ]


    transformed_data = transformed_data[(transformed_data['passenger_count'] > 0) & (transformed_data['trip_distance'] > 0)]

    return transformed_data





