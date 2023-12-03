from sqlalchemy import (
    Column,
    String,
    Integer,
    Boolean,
    Date,
    Sequence,
    BigInteger,
    DateTime,
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


# SQLAlchemy Models
class Profile(Base):
    __tablename__ = "profiles"
    symbol = Column(String(255), primary_key=True, index=True)
    companyName = Column(String(255))
    cik = Column(Integer)
    exchange = Column(String(255))
    exchangeShortName = Column(String(255))
    industry = Column(String(255))
    sector = Column(String(255))
    country = Column(String(255))
    ipoDate = Column(Date)
    defaultImage = Column(Boolean)
    isEtf = Column(Boolean)
    isActivelyTrading = Column(Boolean)


class CashFlow(Base):
    __tablename__ = "cashflows"
    __table_args__ = {"extend_existing": True}

    id = Column(Integer, primary_key=True, autoincrement=True)

    date = Column(String(255))  # Representing period[Q-DEC] as String(255)
    symbol = Column(String(255), index=True)
    reportedCurrency = Column(String(255))
    cik = Column(BigInteger)
    fillingDate = Column(Date)
    acceptedDate = Column(Date)
    calendarYear = Column(BigInteger)
    period = Column(String(255))

    # Columns changed from Integer to BigInteger
    netIncome = Column(BigInteger)
    depreciationAndAmortization = Column(BigInteger)
    deferredIncomeTax = Column(BigInteger)
    stockBasedCompensation = Column(BigInteger)
    changeInWorkingCapital = Column(BigInteger)
    accountsReceivables = Column(BigInteger)
    inventory = Column(BigInteger)
    accountsPayables = Column(BigInteger)
    otherWorkingCapital = Column(BigInteger)
    otherNonCashItems = Column(BigInteger)
    netCashProvidedByOperatingActivities = Column(BigInteger)
    investmentsInPropertyPlantAndEquipment = Column(BigInteger)
    acquisitionsNet = Column(BigInteger)
    purchasesOfInvestments = Column(BigInteger)
    salesMaturitiesOfInvestments = Column(BigInteger)
    otherInvestingActivites = Column(BigInteger)
    netCashUsedForInvestingActivites = Column(BigInteger)
    debtRepayment = Column(BigInteger)
    commonStockIssued = Column(BigInteger)
    commonStockRepurchased = Column(BigInteger)
    dividendsPaid = Column(BigInteger)
    otherFinancingActivites = Column(BigInteger)
    netCashUsedProvidedByFinancingActivities = Column(BigInteger)
    effectOfForexChangesOnCash = Column(BigInteger)
    netChangeInCash = Column(BigInteger)
    cashAtEndOfPeriod = Column(BigInteger)
    cashAtBeginningOfPeriod = Column(BigInteger)
    operatingCashFlow = Column(BigInteger)
    capitalExpenditure = Column(BigInteger)
    freeCashFlow = Column(BigInteger)

    link = Column(String(255))
    finalLink = Column(String(255))


# New model to represent the DataFrame
class MarketCapData(Base):
    __tablename__ = "hist_marketcap"
    id = Column(Integer, primary_key=True, autoincrement=True)

    symbol = Column(String(255), index=True)
    date = Column(DateTime)
    marketCap = Column(BigInteger)


from sqlalchemy import Float


# New model to represent the DataFrame with trading data
class HistPrice(Base):
    __tablename__ = "hist_prices"
    id = Column(Integer, primary_key=True, autoincrement=True)

    symbol = Column(String(255), index=True)
    date = Column(DateTime)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    adjClose = Column(Float)
    volume = Column(BigInteger)
    unadjustedVolume = Column(BigInteger)
    change = Column(Float)
    changePercent = Column(Float)
    vwap = Column(Float)
    label = Column(String(255))
    changeOverTime = Column(Float)