from decimal import Decimal


def safe_div(amount0: Decimal, amount1: Decimal) -> Decimal:
    if amount1 == Decimal(0):
        return Decimal(0)
    else:
        return amount0 / amount1
    

def to_decimal(amount: int, decimals: int) -> Decimal:
    num = exponent_to_decimal(decimals)
    return Decimal(amount) / num


def exponent_to_decimal(decimals: int) -> Decimal:
    return Decimal(10) ** Decimal(decimals)
