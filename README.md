## Generador de Metadata a partir de tablas exceles
***develoment: ELOZN*** 

 El script original se encarga de leer un archivo de Excel de entrada, procesar los datos de los distintos sheets (o "tablas") del archivo y generar un archivo de salida en formato Parquet con información sobre las relaciones de datos entre las tablas.

Para hacerlo, el script utiliza las librerías pandas, pyarrow y json. Además, utiliza la librería difflib para calcular la similitud entre los valores de las columnas de los distintos sheets.

El script se divide en tres partes principales:
  - Obtener el inventario de columnas de cada sheet.
  - Obtener las relaciones de datos entre los sheets.
  - Procesar y guardar los resultados obtenidos en los pasos anteriores.

La versión de la clase **Metadata** tiene métodos equivalentes a cada una de estas tres partes. El método **CreateMetadata** se encarga de obtener el inventario de columnas de cada sheet, el método **GetRalationsDatasets** se encarga de obtener las relaciones de datos entre los sheets y el método **SaveMetadata** se encarga de procesar y guardar los resultados obtenidos en los métodos anteriores.
 ### Ejemplo

 Para utilizar la clase Metadata, primero deberías importarla en tu script:
```python
from metadataCreator import Metadata
```
 Luego, podrías instanciar un objeto de la clase y utilizar sus métodos para procesar el archivo de entrada:
```python
# Instanciar un objeto de la clase
proceso = Metadata()

# Leer el contenido del archivo y sus tablas
proceso.ReadXLSX("ejemplo.xlsx")

# Obtener el inventario de columnas
proceso.CreateMetadata()
proceso.GetDatasets()

# Obtener las relaciones de datos entre los sheets del archivo
proceso.GetRalationsDatasets()

# Guardar los resultados en un archivo de salida
proceso.SaveMetadata("resultados.parquet")
```

También podrías utilizar el método ExportRalitions para exportar los resultados a un archivo de Excel:
```python
proceso.ExportRalitions("resultados")
```


 ### Descripción
 La clase **Metadata** es una clase que se encarga de procesar un archivo de Excel de entrada, obtener información sobre las relaciones de datos entre los sheets del archivo y guardar los resultados en un archivo de salida en formato Parquet.

La clase tiene los siguientes métodos:

 - **__init__**
    La función __init__ es el constructor de la clase en Python. Se utiliza para inicializar los atributos de la clase con los valores proporcionados como argumentos. Es la primera función que se ejecuta cuando se instancia un objeto de la clase.

    En el caso de la clase ***Metadata***, el constructor recibe un argumento:

    file_input: es el nombre del archivo de entrada que se va a procesar.
    El constructor inicializa el atributo self.file_input con el valor del argumento file_input y establece los demás atributos de la clase en valores iniciales vacíos o None. A continuación, te muestro el código completo de la función __init__ de la clase ***Metadata***:
    ```python
    def __init__(self):
        self.filename = None
        self.datasets = None
        self.datanames = None
        self.metadatos = None
        self.inventary = None
        self.data_relation = None
        self.result_data_relation = None
        self.col_data_invent = None
        self.result_col_data_invent = None
    ```

 - **ReadXLSX** (file_input)
  Esta función lee el archivo de entrada proporcionado como argumento y almacena los datos en dataframes en la variable self.datasets. También obtiene el nombre de las hojas de cálculo del archivo y lo almacena en la variable self.datanames.
    ```python
    def ReadXLSX(self, file_input):
        self.filename = file_input #registrado en el archivo de metadata
        xlsx = pd.read_excel(file_input, sheet_name=None)
        self.datasets = list(xlsx.values())
        self.datanames = list(xlsx.keys())
    ```

 - **CreateMetadata** 
    Este método se encarga de leer el archivo de entrada y obtener el inventario de columnas de cada dataset. Para ello, utiliza la librería pandas para leer el archivo de entrada y obtener los datasets y nombres de tablas. Luego, utiliza la librería pyarrow para convertir cada dataset a una tabla y obtener el inventario de columnas de la tabla. Los resultados se almacenan en la variable de instancia self.metadatos.
    ```python
    def CreateMetadata(self, version="1.0.0", title=""):
        current_time = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())

        self.metadatos = {
            'user': os.environ['USERNAME'],
            'version': version,
            'type': title,
            'create': current_time,
            'filename': self.filename,
            'datasets': []
        }

        for dataset, dataname in zip(self.datasets, self.datanames):
            self.table = pa.Table.from_pandas(dataset)
            self.tabla = self.table.schema.metadata
            for k, n in self.tabla.items():
                inventary = json.loads(n.decode())
                inventary["name"] = dataname
                self.metadatos['datasets'].append(inventary)

    ```
 - **SaveMetadata**
    Esta función guarda los resultados del procesamiento en un archivo de salida con el nombre proporcionados como argumento. Primero, serializa el inventario de columnas y lo guarda en el archivo de salida en el formato especificado (json o parquet). Luego, serializa las relaciones de datos.
    ```python
    def SaveMetadata(self, filename="metadatos"):
        with open(filename+".json", "w") as outfile:
            json.dump(self.metadatos, outfile, indent=2)
    ```

 - **GetDatasets** 
    Esta función se encarga de obtener el inventario de columnas de los dataframes del archivo de entrada, que son los nombres de las columnas que aparecen en más de un dataframe. Para hacerlo, se realizan combinaciones entre todos los dataframes y se obtiene la intersección de las columnas de cada combinación. Luego, se eliminan las columnas repetidas y se almacena el resultado en el atributo inventario de la clase.
    ```python
    def GetDatasets(self):
        inventary = []
        for dataset_a, dataname_a in itertools.product(self.datasets, self.datanames):
            for dataset_b, dataname_b in itertools.product(self.datasets, self.datanames):
                if not dataname_a == dataname_b:
                    cols_a = dataset_a.columns.values
                    cols_b = dataset_b.columns.values
                    intersection = np.intersect1d(cols_a, cols_b)
                    self.inventary.extend(intersection)
    ```

 - **GetRalationsDatasets**
    Esta función obtiene las relaciones de los datos de las columnas de los dataframes del archivo de entrada y las almacena en la variable self.data_relation. Primero, se recorren todas las columnas del inventario y luego se comparan los datos de cada columna con todas las demás columnas de todos los dataframes. Si el nombre de la columna es diferente, se calcula la similitud entre los datos de las dos columnas y se almacena en la variable self.data_relation.
    ```python
    def GetRalationsDatasets(self):
        """Obtener relaciones de los datos de las columnas de los dataframes del archivo de entrada"""
        self.data_relation = []
        for col in self.inventary:
            for dataset_a, dataname_a in itertools.product(self.datasets, self.datanames):
                for dataset_b, dataname_b in itertools.product(self.datasets, self.datanames):
                    if not dataname_a == dataname_b:
                        s1 = dataset_a[col].drop_duplicates().values
                        s2 = dataset_b[col].drop_duplicates().values
                        len_s1 = len(s1)
                        len_s2 = len(s2)
                        if not math.isnan(s1).any() and not math.isnan(s2).any():
                            sm1 = round(difflib.SequenceMatcher(None, s1, s2).ratio(), 2)
                            sm2 = round(difflib.SequenceMatcher(None, s2, s1).ratio(), 2)
                            relacion = str(round(((sm1 + sm2) / 2) * 100, 2)) + "%"
                        else:
                            relacion = "Fail"
                            len_s1 = 0
                            len_s2 = 0
                        self.data_relation.append({
                            "column": col,
                            "dataColA": dataname_a,
                            "lenColA": len_s1,
                            "dataColB": dataname_b,
                            "lenColB": len_s2,
                            "distance": relacion
                        })

        self.result_data_relation = pd.DataFrame(self.data_relation)
    ```

 - **ExportRalitions**
    La función utiliza la clase pd.ExcelWriter para crear un archivo de salida en formato Excel con el nombre proporcionado. Luego, utiliza el método to_excel de los dataframes **self.result_col_data_invent** y **self.result_data_relation** para escribir los datos de cada dataframe en una hoja de cálculo diferente del archivo de salida. La hoja de cálculo del dataframe **self.result_col_data_invent** se llama ***Col Data*** y la hoja de cálculo del dataframe **self.result_data_relation** se llama ***Col Relation***. El argumento index=False indica que no se debe escribir el índice de los dataframes en las hojas de cálculo.
    ```python
    def ExportRalitions(self, filename):
        with pd.ExcelWriter(filename +".xlsx") as writer:
            self.result_col_data_invent.to_excel(writer, sheet_name="Col Data", index=False)
            self.result_data_relation.to_excel(writer, sheet_name="Col Relation", index=False)
    ```

#### Fuente y recursos:
[Pandas](https://pandas.pydata.org/)
[PyArrow](https://arrow.apache.org/docs/python/index.html)
