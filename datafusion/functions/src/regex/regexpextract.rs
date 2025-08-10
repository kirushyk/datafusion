use arrow::datatypes::DataType;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use std::any::Any;

#[derive(Debug)]
pub struct RegexpExtractFunc {
    signature: Signature,
}

impl Default for RegexpExtractFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl RegexpExtractFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::exact(vec![Utf8, Utf8, Int64], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for RegexpExtractFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "regexp_extract"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = &args.args;
        let (input, pattern, idx) = match (&args[0], &args[1], &args[2]) {
            (
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(input))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(pattern))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(idx))),
            ) => (input.as_str(), pattern.as_str(), *idx),
            _ => return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
        };

        let re = regex::Regex::new(pattern)
            .map_err(|e| DataFusionError::Execution(format!("Invalid regex: {e}")))?;

        let result = re
            .captures(input)
            .and_then(|caps| caps.get(idx as usize))
            .map(|m| m.as_str().to_string());

        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};

    fn regexp_extract_with_args(
        input: &str,
        pattern: &str,
        idx: i64,
    ) -> Result<ColumnarValue> {
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(input.to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(pattern.to_string()))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(idx))),
            ],
            arg_fields: vec![
                Field::new("input", DataType::Utf8, true).into(),
                Field::new("pattern", DataType::Utf8, true).into(),
                Field::new("idx", DataType::Int64, true).into(),
            ],
            number_rows: 3,
            return_field: Field::new("f", DataType::Utf8, true).into(),
        };
        RegexpExtractFunc::new().invoke_with_args(args)
    }

    fn unwrap_columnar_value(result: ColumnarValue) -> Option<String> {
        if let ColumnarValue::Scalar(ScalarValue::Utf8(ref actual)) = result {
            actual.clone()
        } else {
            unreachable!()
        }
    }

    #[test]
    fn test_regexp_extract_groups() {
        let cases = [
            (0, Some("100-200")),
            (1, Some("100")),
            (2, Some("200")),
            (3, None),
        ];
        for (idx, expected) in cases {
            let result = regexp_extract_with_args("100-200", r"(\d+)-(\d+)", idx);
            let result = unwrap_columnar_value(result.unwrap());
            assert_eq!(result, expected.map(|s| s.to_string()));
        }
    }

    #[test]
    fn test_regexp_extract_null_propagation() {
        let arg_fields = vec![
            Field::new("input", DataType::Utf8, true).into(),
            Field::new("pattern", DataType::Utf8, true).into(),
            Field::new("idx", DataType::Int64, true).into(),
        ];
        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("100-200".into())));
        let pattern =
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(r"(\d+)-(\d+)".into())));
        let idx = ColumnarValue::Scalar(ScalarValue::Int64(Some(1)));
        let null = ColumnarValue::Scalar(ScalarValue::Null);
        let return_field: std::sync::Arc<Field> =
            Field::new("f", DataType::Utf8, true).into();
        let args = ScalarFunctionArgs {
            args: vec![null.clone(), pattern.clone(), idx.clone()],
            arg_fields: arg_fields.clone(),
            number_rows: 3,
            return_field: return_field.clone(),
        };
        let result = RegexpExtractFunc::new().invoke_with_args(args);
        let result = unwrap_columnar_value(result.unwrap());
        assert_eq!(result, None);

        let args = ScalarFunctionArgs {
            args: vec![input.clone(), null.clone(), idx.clone()],
            arg_fields: arg_fields.clone(),
            number_rows: 3,
            return_field: return_field.clone(),
        };
        let result = RegexpExtractFunc::new().invoke_with_args(args);
        let result = unwrap_columnar_value(result.unwrap());
        assert_eq!(result, None);

        let args = ScalarFunctionArgs {
            args: vec![input.clone(), pattern.clone(), null.clone()],
            arg_fields: arg_fields.clone(),
            number_rows: 3,
            return_field: return_field.clone(),
        };
        let result = RegexpExtractFunc::new().invoke_with_args(args);
        let result = unwrap_columnar_value(result.unwrap());
        assert_eq!(result, None);

        let args = ScalarFunctionArgs {
            args: vec![null.clone(), null.clone(), null.clone()],
            arg_fields: arg_fields.clone(),
            number_rows: 3,
            return_field: return_field.clone(),
        };
        let result = RegexpExtractFunc::new().invoke_with_args(args);
        let result = unwrap_columnar_value(result.unwrap());
        assert_eq!(result, None);

        let args = ScalarFunctionArgs {
            args: vec![input.clone(), pattern.clone(), idx.clone()],
            arg_fields: arg_fields.clone(),
            number_rows: 3,
            return_field: return_field.clone(),
        };
        let result = RegexpExtractFunc::new().invoke_with_args(args);
        let result = unwrap_columnar_value(result.unwrap());
        assert_ne!(result, None);
    }
}
