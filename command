hadoop-streaming -files <mapper script path>,<reducer script path> -mapper "python3 mapper.py" -reducer "python3 reducer.py" -input <input path1,input path2> -output <output path
