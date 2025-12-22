from src.core.pipeline import StressPipeline
from src.stress.fund_stress import FundStress
from src.stress.volatility import VolatilityTrigger
from src.stress.pd_acceleration import PDAcceleration

if __name__ == "__main__":
    pipeline = StressPipeline([
        FundStress(),
        VolatilityTrigger(),
        PDAcceleration()
    ])

    result = pipeline.run({"date": "2020-03-15"})
    print(result)

