"""DB-free optional observations; core price and legacy signed mappings stay fixed."""
from datetime import datetime
import math

BAR_METADATA={
    'trade_count':'BIGINT','feed':'VARCHAR','turnover_method':'VARCHAR',
    'vwap_method':'VARCHAR','retrieved_at':'TIMESTAMPTZ',
    'is_tradable_bar':'BOOLEAN','exchange':'VARCHAR',
    'unit_change_code':'VARCHAR','price_adjustment_rate':'DOUBLE',
}
ASSET_METADATA={
    'provider_asset_id':'VARCHAR','provider_asset_class':'VARCHAR',
    'active_at_collection':'BOOLEAN','broker_tradable':'BOOLEAN',
    'universe_source':'VARCHAR','observed_at':'TIMESTAMPTZ',
}


def validate_metadata(row,contract):
    """Validate only fields supplied, without inserting defaults into signed data."""
    for name,kind in contract.items():
        if name not in row or row[name] is None:continue
        value=row[name]
        if kind=='BIGINT':
            valid=type(value) is int and 0<=value<=2**63-1
        elif kind=='BOOLEAN':valid=type(value) is bool
        elif kind=='VARCHAR':valid=isinstance(value,str) and len(value)<=1024 and '\x00' not in value
        elif kind=='DOUBLE':
            try:valid=type(value) in (int,float) and math.isfinite(value)
            except OverflowError:valid=False
        else:
            valid=False
            if isinstance(value,str) and len(value)<=64:
                try:
                    stamp=datetime.fromisoformat(value.replace('Z','+00:00'))
                    valid=stamp.tzinfo is not None and stamp.utcoffset().total_seconds()==0
                except ValueError:pass
        if not valid:raise ValueError('Invalid optional price metadata: '+name)


def metadata_columns(frame,core,contract):
    """Project allowlisted fields actually present; omission remains omission."""
    return list(core)+[name for name in contract if name in frame and name not in core]


def normalize_frame_metadata(frame,contract):
    """Restore nullable integer columns after DataFrame JSON/parquet projection.

    This applies only to known DataFrame integer metadata. Wire JSON retains
    strict integer validation, including rejection of integral floats and bools.
    """
    import pandas as pd
    result=frame.copy()
    for name,kind in contract.items():
        if kind!='BIGINT' or name not in result:continue
        values=[]
        for value in result[name]:
            if value is None or value is pd.NA or pd.isna(value):
                values.append(None);continue
            if hasattr(value,'item'):value=value.item()
            if type(value) is float and value.is_integer():value=int(value)
            validate_metadata({name:value},{name:kind})
            values.append(value)
        result[name]=pd.array(values,dtype='Int64')
    return result


def benchmark_unit_flags_supported(code,rate):
    """Named KIS ETF raw-price evidence only: cash ex-dates never alter units.

    KIS official master codes: 00 none, 02 dividend, 03 distribution. The chart's
    generic adjustment rate is not a share-unit factor on cash ex-dates.
    """
    if type(rate) not in (int,float):return False
    try:finite=math.isfinite(rate)
    except OverflowError:return False
    return finite and (code=='00' and rate==0 or code in ('02','03'))
