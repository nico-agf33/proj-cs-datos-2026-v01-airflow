from autos.normalize import as_number, parse_motor


def test_argentine_and_decimal_number_formats():
    assert as_number("157.000 km") == 157000.0
    assert as_number("$ 31.500.000") == 31500000.0
    assert as_number("9,3 lts / 100km") == 9.3
    assert as_number("1.6 lts") == 1.6


def test_motor_liters_preserve_decimal_separator():
    assert parse_motor("1.3") == 1.3
    assert parse_motor("1600 cc") == 1.6
