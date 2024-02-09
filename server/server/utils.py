from decimal import Decimal

from bson import Decimal128


async def safe_div(amount0: Decimal, amount1: Decimal) -> Decimal:
    if amount1 == Decimal(0):
        return Decimal(0)
    else:
        return amount0 / amount1
    

async def amount_after_decimals(amount: int, decimals: int) -> Decimal:
    num = await exponent_to_decimal(decimals)
    return Decimal(amount) / num


async def exponent_to_decimal(decimals: int) -> Decimal:
    return Decimal(10) ** Decimal(decimals)


async def format_address(address: str) -> str:
    return hex(int(address, 16))


async def convert_num_to_decimal128(num: int | float | Decimal) -> str:
    if not isinstance(num, Decimal):
        num = Decimal(num)
    if not isinstance(num, Decimal128):
        num = Decimal128(num)
    return num
