class StressPipeline:
    def __init__(self, stages):
        self.stages = stages

    def run(self, data):
        state = data
        for stage in self.stages:
            state = stage.run(state)
        return state

