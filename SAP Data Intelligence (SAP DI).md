# SAP Data Intelligence (SAP DI)

## Building a  simple pipeline to train a ML Model{#buildingapipeline}

After the exploratory part (data exploration & freestyle data science) in Jupyter Labs, the final developments can be delivered via pipelines. A simple pipeline can be put together from just a a few steps and in this case is exemplarily constructed as follows[^1]:

[^1]: The original content includes also the data exploration and freestyle data science parts. This guide includes updates and some more in depth content on the settings for the operators. The original content is available here: https://blogs.sap.com/2019/08/14/sap-data-intelligence-create-your-first-ml-scenario/#freestyle

![2020-04-16 15_17_41-Modeler _ SAP Data Intelligence](F:\OneDrive - Verovis GmbH\Projekte\SAP Data Intelligence\Marathon_Times_Tutorial\Bilder\2020-04-16 15_17_41-Modeler _ SAP Data Intelligence.png)

### Operator Settings {#operatorsettings}

#### Read File {#readfile}

- [ ] Read: Set to **once** 

- [ ] Connection: Chose the connection via the **connection management** drop down menu. To access the DI Data lake, the settings are as follows


![2020-04-16 16_01_19-Modeler _ SAP Data Intelligence](F:\OneDrive - Verovis GmbH\Projekte\SAP Data Intelligence\Marathon_Times_Tutorial\Bilder\2020-04-16 16_01_19-Modeler _ SAP Data Intelligence.png)

- [ ] Path: contains the path to the file, either give a direct path or use `${inputFilePath}` to be able to provide the path and file when executing the pipeline

#### To String Converter{#tostringconverter}

* Preconfigured and no actions needed

#### Python {#python}

Select the *python3 operator* from the operators menu and drag and drop it to the canvas. The operator comes without **input** or **output ports**, thus they have to be added to the python operator.

##### Port Settings {#portsettings}

The pipeline uses **one** input file for the data input that is leveraged in our python operator to train the model. The python operator generates **two** outputs (metrics and model) for further use in the pipeline. 

To add ports click on the python operator and select the **Add Port** icon

![2020-04-16 16_08_50-Modeler _ SAP Data Intelligence](F:\OneDrive - Verovis GmbH\Projekte\SAP Data Intelligence\Marathon_Times_Tutorial\Bilder\2020-04-16 16_08_50-Modeler _ SAP Data Intelligence.png)

- [ ] The settings for the input port are as follows


![2020-04-16 16_10_29-Modeler _ SAP Data Intelligence](F:\OneDrive - Verovis GmbH\Projekte\SAP Data Intelligence\Marathon_Times_Tutorial\Bilder\2020-04-16 16_10_29-Modeler _ SAP Data Intelligence.png)

- [ ] The settings for the output port are as follows


![2020-04-16 16_10_52-Modeler _ SAP Data Intelligence](F:\OneDrive - Verovis GmbH\Projekte\SAP Data Intelligence\Marathon_Times_Tutorial\Bilder\2020-04-16 16_10_52-Modeler _ SAP Data Intelligence.png)

![2020-04-16 16_11_07-Modeler _ SAP Data Intelligence](F:\OneDrive - Verovis GmbH\Projekte\SAP Data Intelligence\Marathon_Times_Tutorial\Bilder\2020-04-16 16_11_07-Modeler _ SAP Data Intelligence.png)

##### Code (Script) Settings {#codesettings}

Access the script settings of the python3 operator 

![2020-04-20 10_23_32-Window](F:\OneDrive - Verovis GmbH\Projekte\SAP Data Intelligence\Marathon_Times_Tutorial\Bilder\2020-04-20 10_23_32-Window.png)

and replace the exemplary  code with the following:

```python
# Example Python script to perform training on input data & generate Metrics & Model Blob
def on_input(data):
    
    # Obtain data
    import pandas as pd
    import io
    import pickle
    df_data = pd.read_csv(io.StringIO(data), sep=";")
    
    # Get predictor and target
    x = df_data[["HALFMARATHON_MINUTES"]]
    y_true = df_data["MARATHON_MINUTES"]
    
    # Train regression
    from sklearn.linear_model import LinearRegression
    lm = LinearRegression()
    lm.fit(x, y_true)
    
    # Model quality
    import numpy as np
    y_pred = lm.predict(x)
    mse = mean_squared_error(y_true, y_pred)
    rmse = round(np.sqrt(mse), 2)
    
    # to send metrics to the Submit Metrics operator, create a Python dictionary of 	key-value pairs
    metrics_dict = {"RMSE": str(rmse), "n": str(len(df_data))}
    
    # send the metrics to the output port - Submit Metrics operator will use this to 	persist the metrics 
    api.send("metrics", api.Message(metrics_dict))

    # create & send the model blob to the output port - Artifact Producer operator 		will use this to persist the model and create an artifact ID
    model_blob = pickle.dumps(lm)
    api.send("modelBlob", model_blob)

api.set_port_callback("input", on_input)
```

The *python3 operator* receives the data from the *read file operator* and then uses the developed code to train the model. The model metrics are calculated, saved as a dictionary and then send to the output port and passed to the ML Scenario. The ML model is saved as pickle file and then send to the *modelBlob output port* where the *artifact produces operator* consumes the ML model, persists it and creates an artifact ID which is also passed to the ML Scenario.

##### Submit Metrics {#submitmetrics}

- Preconfigured and no actions needed

##### Artifact Producer {#artifactproducer}

- Preconfigured and no actions needed

##### Operators Complete {#operatorscompleted}

We need to check if all actions were carried out successfully and terminate the graph afterwards. Select the *python3 operator* from the operators menu and drag and drop it to the canvas. We need two input (metrics & ML model) and one output (completed message) port. The ports can be added in the same way again [Port Settings](#portsettings).