use arrow::datatypes::DataType;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
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

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let args = &args.args;
        let (input, pattern, idx) = match (&args[0], &args[1], &args[2]) {
            (
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(input))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(pattern))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(idx))),
            ) => (input.as_str(), pattern.as_str(), *idx),
            _ => unreachable!(),
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
