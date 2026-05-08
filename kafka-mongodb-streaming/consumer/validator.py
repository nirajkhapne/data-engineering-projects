import re


def validate(record):

    required_fields = [
        "BookingID",
        "vehicle_no"
    ]

    for field in required_fields:
        if field not in record or record[field] is None:
            raise ValueError(f"Missing field: {field}")

    if not isinstance(record["BookingID"], str):
        raise ValueError("BookingID must be string")

    mobile = record.get("Driver_MobileNo")

    if mobile and not re.match(r"^[0-9]{10}$", mobile):
        raise ValueError("Invalid mobile number")

    return True
