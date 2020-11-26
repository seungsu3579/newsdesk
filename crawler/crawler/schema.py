import pyarrow as pa

Schema = {
    "date":"Date",
    "time":"String",
    "category":"String",
    "author":"String",
    "company":"String",
    "headline":"String",
    "sentence":"String",
    "content_url":"String",
    "image_url":"String",
}

def field_data_type_mapper(data_type: str):
    if data_type == 'String':
        return pa.string()
    elif data_type == 'Double':
        return pa.float64()
    elif data_type == 'Long':
        return pa.int64()
    ## date 부분 수정필요
    elif data_type == 'Date':
        return pa.int64()
    elif data_type == 'Timestamp':
        return pa.timestamp('ns')
    elif data_type == 'Boolean':
        return pa.bool_()
    else:
        raise ValueError


def field_mapper(column_name: str):
    data_type = Schema[column_name]
    return pa.field(column_name, field_data_type_mapper(data_type))


def generate_schema(parquet_table):
    fields = []
    for column_name in parquet_table.schema:
        column = column_name.name
        fields.append(field_mapper(column))
    return pa.schema(fields)