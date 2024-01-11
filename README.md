<!DOCTYPE html>
<html lang="en">

<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>

<body>

  <h1>Vehicle Crash Data Analysis with PySpark</h1>

  <h2>Overview</h2>

  <p>This repository contains Python code for analyzing a vehicle crash dataset using PySpark. The analysis addresses various questions related to the dataset, and the functionality is encapsulated within the <code>Class_car_crash.py</code> script. The <code>run_script.py</code> script is used to instantiate the class, run the analysis functions, and save the results as a CSV file.</p>

  <h2>Contents</h2>

  <ul>
    <li><code>Class_car_crash.py</code>: Python script containing a class with functions for loading, analyzing, and saving the vehicle crash dataset using PySpark.</li>
    <li><code>run_script.py</code>: Python script to instantiate the class from <code>Class_car_crash.py</code>, execute the analysis functions, and save the analysis results as a CSV file.</li>
  </ul>

  <h2>Requirements</h2>

  <ul>
    <li>Python 3.x</li>
    <li>PySpark</li>
  </ul>

  <h2>Setup</h2>

  <ol>
    <li>Clone the repository:</li>
    <pre><code>git clone https://github.com/your-username/vehicle-crash-analysis.git
cd vehicle-crash-analysis</code></pre>
    <li>Install the required dependencies:</li>
    <pre><code>pip install -r requirements.txt</code></pre>
    <p>Note: Ensure you have PySpark installed. You may need to configure the <code>HADOOP_HOME</code> environment variable if using Hadoop-related functionality.</p>
  </ol>

  <h2>Usage</h2>

  <ol>
    <li>Run the analysis script:</li>
    <pre><code>python run_script.py</code></pre>
    <p>This will load the dataset, perform the analysis, and save the results as a CSV file.</p>
  </ol>

  <h2>Results</h2>

  <p>The analysis results will be saved in a CSV file inside <code>output</code> folder. You can find the detailed results of each analysis question in this file.</p>

  <h2>Author</h2>

  <p>[Aman Chauhan]</p>

</body>

</html>
