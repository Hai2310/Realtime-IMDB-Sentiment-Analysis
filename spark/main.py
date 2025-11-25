from orchestration import ImdbPipeline
import traceback
import sys

sys.path.append("/home/enovo/prj/test/spark")

def main() :
    print("\n" + "="*90)
    print("=== IMDB ANALYSIS SYSTEM ===")
    print("="*90)
    try :
        Pipeline = ImdbPipeline()


        Pipeline.load_data()
        Pipeline.run_all_analysis()
        Pipeline.save_sql()

        print("\n" + "="*90)
        print("=== ALL PIPELINE COMPLETED ===")
        print("="*90)
        sys.exit(0)

    except Exception as e :
        print("Pipeline failed : " , str(e))
        traceback.print_exc()
        sys.exit(1)
        
if __name__ == "__main__" :
    main()