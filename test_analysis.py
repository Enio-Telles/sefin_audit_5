import sys
sys.path.insert(0, 'server/python')
try:
    from routers import analysis
    print("Successfully imported analysis")
except Exception as e:
    import traceback
    traceback.print_exc()
