from databricks.sdk.service.sql import StatementResponse, ColumnInfo


def get_results_dict(response: StatementResponse):
    def apply_casting(columns: list[ColumnInfo], row: list[str]):
        new_row = []

        for i in range(len(columns)):
            if (row[i] is None):
                new_row.append(None)
            else:
                column = columns[i]

                match column.type_text:
                    case "INT":
                        new_row_value = int(row[i])
                    case "BOOLEAN":
                        new_row_value = row[i] == "true"
                    case _:
                        new_row_value = row[i]

                new_row.append(new_row_value)

        return new_row

    data = map(
        lambda row: apply_casting(response.manifest.schema.columns, row),
        response.result.data_array
    )

    column_names = [col.name for col in response.manifest.schema.columns]

    return [dict(zip(column_names, row)) for row in data]
