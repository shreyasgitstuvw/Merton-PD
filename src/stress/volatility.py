class VolatilityTrigger:
    def run(self, data):
        data["volatility_trigger"] = False
        return data

