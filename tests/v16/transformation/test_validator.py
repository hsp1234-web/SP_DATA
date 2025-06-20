import unittest
from unittest.mock import patch
import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal
import numpy as np # For NaN

# Add src to sys.path to allow direct import of modules under src
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from src.sp_data_v16.transformation.validator import DataValidator

class TestDataValidator(unittest.TestCase):

    def setUp(self):
        """Initialize DataValidator instance for all tests."""
        self.validator = DataValidator()

    def test_successful_type_conversion(self):
        """Test successful conversion to integer, float, datetime."""
        data = {
            'col_int': ['1', '2', '3'],
            'col_float': ['10.1', '20.2', '30.3'],
            'col_datetime': ['2023-01-01', '2023-01-02', '2023-01-03'],
            'col_string': ['a', 'b', 'c']
        }
        df = pd.DataFrame(data)
        schema = {
            'columns': {
                'col_int': {'dtype': 'integer'},
                'col_float': {'dtype': 'float'},
                'col_datetime': {'dtype': 'datetime'},
                'col_string': {'dtype': 'string'} # String type, no specific conversion
            }
        }

        validated_df = self.validator.validate(df.copy(), schema)

        self.assertEqual(validated_df['col_int'].dtype, 'Int64')
        self.assertEqual(validated_df['col_float'].dtype, 'float64')
        self.assertEqual(validated_df['col_datetime'].dtype, 'datetime64[ns]')
        self.assertEqual(validated_df['col_string'].dtype, 'object') # Stays as object for strings

        expected_data = {
            'col_int': pd.Series([1, 2, 3], dtype='Int64'),
            'col_float': pd.Series([10.1, 20.2, 30.3], dtype='float64'),
            'col_datetime': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03']),
            'col_string': pd.Series(['a', 'b', 'c'], dtype='object')
        }
        expected_df = pd.DataFrame(expected_data)
        assert_frame_equal(validated_df, expected_df, check_dtype=True)

    def test_type_conversion_errors_coerce_nan(self):
        """Test that unconvertible values are coerced to NaN/NaT."""
        data = {
            'col_int': ['1', 'abc', '3'],
            'col_float': ['10.1', 'xyz', '30.3'],
            'col_datetime': ['2023-01-01', 'invalid_date', '2023-01-03']
        }
        df = pd.DataFrame(data)
        schema = {
            'columns': {
                'col_int': {'dtype': 'integer'},
                'col_float': {'dtype': 'float'},
                'col_datetime': {'dtype': 'datetime'}
            }
        }

        with patch('builtins.print') as mocked_print:
            validated_df = self.validator.validate(df.copy(), schema)

            self.assertTrue(pd.isna(validated_df['col_int'].iloc[1]))
            self.assertTrue(pd.isna(validated_df['col_float'].iloc[1]))
            self.assertTrue(pd.isna(validated_df['col_datetime'].iloc[1])) # NaT for datetime

            self.assertEqual(validated_df['col_int'].iloc[0], 1)
            self.assertEqual(validated_df['col_float'].iloc[0], 10.1)
            self.assertEqual(validated_df['col_datetime'].iloc[0], pd.Timestamp('2023-01-01'))

            # Check if warnings were printed for conversion errors
            # Example: "Warning: Column 'col_int' (dtype: integer) had values that could not be converted..."
            self.assertTrue(any("col_int" in call.args[0] and "could not be converted" in call.args[0] for call in mocked_print.call_args_list))
            self.assertTrue(any("col_float" in call.args[0] and "could not be converted" in call.args[0] for call in mocked_print.call_args_list))
            self.assertTrue(any("col_datetime" in call.args[0] and "could not be converted" in call.args[0] for call in mocked_print.call_args_list))


    def test_non_nullable_field_with_nulls(self):
        """Test non-nullable field check when nulls are present (original or after coercion)."""
        data = {
            'col_non_nullable_original_null': [1, None, 3],
            'col_non_nullable_becomes_null': ['10', 'abc', '30'] # 'abc' will become NaN
        }
        df = pd.DataFrame(data)
        schema = {
            'columns': {
                'col_non_nullable_original_null': {'dtype': 'integer', 'nullable': False},
                'col_non_nullable_becomes_null': {'dtype': 'integer', 'nullable': False}
            }
        }

        with patch('builtins.print') as mocked_print:
            validated_df = self.validator.validate(df.copy(), schema)

            self.assertTrue(pd.isna(validated_df['col_non_nullable_original_null'].iloc[1]))
            self.assertTrue(pd.isna(validated_df['col_non_nullable_becomes_null'].iloc[1]))

            # Check for warnings
            # Example: "Warning: Column 'col_non_nullable_original_null' is not nullable but contains NaN values..."
            self.assertTrue(any("col_non_nullable_original_null" in call.args[0] and "not nullable but contains NaN" in call.args[0] for call in mocked_print.call_args_list))
            self.assertTrue(any("col_non_nullable_becomes_null" in call.args[0] and "not nullable but contains NaN" in call.args[0] for call in mocked_print.call_args_list))

    def test_nullable_field_with_nulls(self):
        """Test nullable field handling: nulls are allowed and preserved."""
        data = {
            'col_nullable': [1.0, None, 3.0, np.nan, 'text_will_be_nan'],
        }
        df = pd.DataFrame(data)
        schema = {
            'columns': {
                'col_nullable': {'dtype': 'float', 'nullable': True}
            }
        }

        with patch('builtins.print') as mocked_print:
            validated_df = self.validator.validate(df.copy(), schema)

            self.assertEqual(validated_df['col_nullable'].iloc[0], 1.0)
            self.assertTrue(pd.isna(validated_df['col_nullable'].iloc[1])) # Original None
            self.assertEqual(validated_df['col_nullable'].iloc[2], 3.0)
            self.assertTrue(pd.isna(validated_df['col_nullable'].iloc[3])) # Original NaN
            self.assertTrue(pd.isna(validated_df['col_nullable'].iloc[4])) # 'text_will_be_nan' coerced to NaN

            # Check that NO "not nullable" warnings were printed for this column
            self.assertFalse(any("col_nullable" in call.args[0] and "not nullable" in call.args[0] for call in mocked_print.call_args_list))
            # A conversion warning for 'text_will_be_nan' is expected though
            self.assertTrue(any("col_nullable" in call.args[0] and "could not be converted" in call.args[0] for call in mocked_print.call_args_list))


    def test_column_in_schema_not_in_df(self):
        """Test that schema columns not in DataFrame are handled with a warning."""
        data = {'col_existing': [1, 2]}
        df = pd.DataFrame(data)
        schema = {
            'columns': {
                'col_existing': {'dtype': 'integer'},
                'col_missing': {'dtype': 'string', 'nullable': False}
            }
        }

        with patch('builtins.print') as mocked_print:
            validated_df = self.validator.validate(df.copy(), schema)

            self.assertNotIn('col_missing', validated_df.columns)
            self.assertIn('col_existing', validated_df.columns)

            # Check for warning about missing column
            self.assertTrue(any("'col_missing'" in call.args[0] and "defined in schema but not found" in call.args[0] for call in mocked_print.call_args_list))

    def test_empty_dataframe(self):
        """Test handling of an empty DataFrame."""
        df_empty = pd.DataFrame(columns=['col_a', 'col_b'])
        schema = {
            'columns': {
                'col_a': {'dtype': 'integer'},
                'col_b': {'dtype': 'string'}
            }
        }

        validated_df = self.validator.validate(df_empty.copy(), schema)

        self.assertTrue(validated_df.empty)
        # The validator does not add columns if they don't exist in the input df, even if in schema.
        # It processes existing columns. So, validated_df will have the same columns as df_empty.
        assert_series_equal(pd.Series(validated_df.columns), pd.Series(df_empty.columns), check_dtype=False)
        # Check that dtypes are converted for existing columns if any (though df_empty has no data for dtypes to be inferred beyond object)
        # If an empty dataframe with defined columns but no data is passed, pandas often defaults to 'object' dtype.
        # Our validator will try to convert them based on schema, resulting in correct (empty) typed Series.
        self.assertEqual(validated_df['col_a'].dtype, 'Int64')
        self.assertEqual(validated_df['col_b'].dtype, 'object') # String type

    def test_correct_types_no_change(self):
        """Test that DataFrames with already correct types are not altered."""
        data = {
            'col_int': pd.Series([1, 2, 3], dtype='Int64'),
            'col_float': pd.Series([10.1, 20.2, 30.3], dtype='float64'),
            'col_datetime': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03'])
        }
        df = pd.DataFrame(data)
        original_df_copy = df.copy() # Keep a pristine copy for comparison

        schema = {
            'columns': {
                'col_int': {'dtype': 'integer'},
                'col_float': {'dtype': 'float'},
                'col_datetime': {'dtype': 'datetime'}
            }
        }

        validated_df = self.validator.validate(df.copy(), schema) # Pass a copy to validate
        assert_frame_equal(validated_df, original_df_copy, check_dtype=True)

    def test_int64_conversion_supports_nan(self):
        """Test integer conversion uses Int64 to support NaN."""
        data = {'col_int_nan': ['1', 'invalid', None, '3']}
        df = pd.DataFrame(data)
        schema = {'columns': {'col_int_nan': {'dtype': 'integer'}}}

        validated_df = self.validator.validate(df.copy(), schema)

        self.assertEqual(validated_df['col_int_nan'].dtype, 'Int64')
        self.assertEqual(validated_df['col_int_nan'].iloc[0], 1)
        self.assertTrue(pd.isna(validated_df['col_int_nan'].iloc[1])) # 'invalid' becomes NaN
        self.assertTrue(pd.isna(validated_df['col_int_nan'].iloc[2])) # None remains NaN
        self.assertEqual(validated_df['col_int_nan'].iloc[3], 3)

    def test_datetime_conversion_with_mixed_formats_and_errors(self):
        """Test datetime conversion with various formats and error handling."""
        # Simplified data to isolate the parsing of '2023-01-03 10:00:00'
        data = {
            'dt_col': [
                '2023-01-03 10:00:00',
                '2023-01-01 00:00:00', # Using consistent full datetime format
                'invalid_date'       # Known invalid
            ]
        }
        df = pd.DataFrame(data)
        schema = {'columns': {'dt_col': {'dtype': 'datetime'}}}

        with patch('builtins.print') as mocked_print:
            validated_df = self.validator.validate(df.copy(), schema)

            self.assertEqual(validated_df['dt_col'].dtype, 'datetime64[ns]')
            self.assertEqual(validated_df['dt_col'].iloc[0], pd.Timestamp('2023-01-03 10:00:00'))
            self.assertEqual(validated_df['dt_col'].iloc[1], pd.Timestamp('2023-01-01 00:00:00'))
            self.assertTrue(pd.isna(validated_df['dt_col'].iloc[2])) # 'invalid_date' becomes NaT

            # Check that a warning for "invalid_date" is present
            self.assertTrue(
                any("dt_col" in call.args[0] and
                    "could not be converted" in call.args[0] and
                    "invalid_date" in str(call.args)
                    for call in mocked_print.call_args_list),
                "Warning for 'invalid_date' not found or incorrect."
            )

if __name__ == '__main__':
    unittest.main()
