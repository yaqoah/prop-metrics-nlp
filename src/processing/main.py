from src.processing.pipeline.orchestrator import Orchestrator

def main():
    # initialize processing pipeline orchestration
    orchestrator = Orchestrator()

    # efficiently process the data  
    orchestrator.run_pipeline()

if __name__ == "__main__":
    main()


# terminal command for run: 
# python -m src.processing.main
