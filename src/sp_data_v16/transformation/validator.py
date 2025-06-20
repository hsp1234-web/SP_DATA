import pandas as pd

class DataValidator:
    def validate(self, dataframe: pd.DataFrame, schema: dict) -> pd.DataFrame:
        """
        Validates the DataFrame based on the provided schema.

        Args:
            dataframe: The pandas DataFrame to validate.
            schema: A dictionary defining the schema with 'columns' information.

        Returns:
            A pandas DataFrame with data types converted and nullability checked.
        """
        processed_df = dataframe.copy()
        warnings = []
        has_critical_error = False # Flag for critical validation errors

        for column_name, col_schema in schema.get('columns', {}).items():
            if column_name not in processed_df.columns:
                warnings.append(f"Warning: Column '{column_name}' defined in schema but not found in DataFrame.")
                continue

            target_dtype = col_schema.get('dtype')
            nullable = col_schema.get('nullable', True)

            if target_dtype == 'integer':
                processed_df[column_name] = pd.to_numeric(processed_df[column_name], errors='coerce').astype('Int64') # Use Int64 to support NaN
            elif target_dtype == 'float':
                processed_df[column_name] = pd.to_numeric(processed_df[column_name], errors='coerce')
            elif target_dtype == 'datetime':
                processed_df[column_name] = pd.to_datetime(processed_df[column_name], errors='coerce')
            # Add other type conversions here if needed, e.g., string, boolean

            # Check for NaN values after conversion
            if target_dtype in ['integer', 'float', 'datetime']:
                conversion_errors = processed_df[column_name].isna() & ~dataframe[column_name].isna()
                if conversion_errors.any():
                    error_indices = conversion_errors[conversion_errors].index.tolist()
                    original_values = dataframe[column_name][error_indices].tolist()
                    warnings.append(
                        f"Warning: Column '{column_name}' (dtype: {target_dtype}) had values that could not be converted. "
                        f"Indices: {error_indices}. Original values: {original_values}. These were set to NaN."
                    )

            # Nullability check
            if not nullable:
                if processed_df[column_name].isna().any():
                    nan_indices = processed_df[column_name][processed_df[column_name].isna()].index.tolist()
                    warnings.append(
                        f"Critical: Column '{column_name}' is defined as non-nullable but contains NaN values at indices: {nan_indices}."
                        " These NaN values might be due to original data or conversion errors."
                    )
                    has_critical_error = True # Set critical error flag

        # Print all warnings
        for warning in warnings:
            print(warning)

        if has_critical_error:
            print("Critical validation errors found. Returning None.")
            return None

        return processed_df

# Example Usage (optional, for testing)
if __name__ == '__main__':
    # Sample schema
    sample_schema = {
        'columns': {
            'id': {'dtype': 'integer', 'nullable': False},
            'name': {'dtype': 'string', 'nullable': False},
            'value': {'dtype': 'float', 'nullable': True},
            'event_date': {'dtype': 'datetime', 'nullable': False},
            'category': {'dtype': 'string', 'nullable': True}
        }
    }

    # Sample DataFrame
    data = {
        'id': [1, 2, 'invalid_id', 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', None, 'Eve'], # Eve will be an issue due to nullable: False
        'value': [10.5, 'not_a_float', 30.2, 40.5, None],
        'event_date': ['2023-01-01', '2023-01-02', 'invalid_date', '2023-01-04', None], # None will be an issue
        'extra_column': [1,2,3,4,5] # This column is not in schema
    }
    df = pd.DataFrame(data)

    print("Original DataFrame:")
    print(df)
    print("\nOriginal DataTypes:")
    print(df.dtypes)

    validator = DataValidator()
    validated_df = validator.validate(df.copy(), sample_schema) # Use df.copy() to avoid modifying original df in example

    print("\nValidated DataFrame:")
    print(validated_df)
    print("\nValidated DataTypes:")
    print(validated_df.dtypes)

    # Example with a column missing from DataFrame that is in schema
    sample_schema_missing_col = {
        'columns': {
            'id': {'dtype': 'integer', 'nullable': False},
            'non_existent_col': {'dtype': 'string', 'nullable': False},
        }
    }
    df_simple = pd.DataFrame({'id': [1, 'bad', 3]})
    print("\n--- Testing missing column from DataFrame ---")
    print("Original DataFrame:")
    print(df_simple)
    validator.validate(df_simple.copy(), sample_schema_missing_col)

    # Example with Int64 for integer that can hold NaNs
    print("\n--- Testing integer with NaN ---")
    schema_int_nan = {
        'columns': {
            'int_col': {'dtype': 'integer', 'nullable': True},
            'int_col_non_nullable': {'dtype': 'integer', 'nullable': False}
        }
    }
    df_int_nan = pd.DataFrame({
        'int_col': [1, 'error', 3, None],
        'int_col_non_nullable': [1, 2, 'error', None]
    })
    print("Original DataFrame:")
    print(df_int_nan)
    validated_int_df = validator.validate(df_int_nan.copy(), schema_int_nan)
    print("\nValidated DataFrame (Integer with NaN):")
    print(validated_int_df)
    print(validated_int_df.dtypes)
