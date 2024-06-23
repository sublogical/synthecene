use crate::porta_api::NumberDataPoint;
use crate::porta_api::number_data_point::Value;

pub(crate) mod auto;

pub(crate) fn format_datapoint(val: &NumberDataPoint) -> String {
    
    match val.value {
        Some(Value::AsDouble(fval)) => {
            format!("{:.4}", fval)
        },
        Some(Value::AsInt(ival)) => {
            format!("{:.4}", ival)
        },
        None => {
            panic!("nah");
        }
    }
}

pub(crate) fn format_metric(vals: &Vec<NumberDataPoint>) -> String {
    let num_datapoints = vals.len();

    if num_datapoints == 0 {
        return "[]".to_string();
    }

    if num_datapoints == 1 {
        return format!("[{}]", format_datapoint(&vals.first().unwrap()));
    }

    let width = termsize::get().map(|size| size.cols).unwrap_or(80) as usize;
    let last = format_datapoint(&vals.last().unwrap());

    let remaining = width - (2 + last.len());
    let mut left = String::with_capacity(width as usize);
    left.push('[');

    for index in 0..num_datapoints {
        let current = format_datapoint(&vals[index]);
        let reserve = if index == num_datapoints - 2 {
            1 // just need comma if the last to fit is the second to last
        } else {
            3 // need ellipsis before that
        };

        if (index != num_datapoints - 1) && (left.len() + current.len() + reserve > remaining) {
            left.push_str(&"...");
            left.push_str(&last);
            break;
        } else {
            if index > 0 {
                left.push(',');
            }
            left.push_str(&current);
        }
    }
    left.push(']');
    left
}
