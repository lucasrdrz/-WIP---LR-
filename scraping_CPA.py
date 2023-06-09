import requests
from bs4 import BeautifulSoup
import csv
import pandas as pd
import concurrent.futures
from lxml import html

# Obtener las provincias desde la página web
url = "https://codigo-postal.co/argentina/"
response = requests.get(url)
soup = BeautifulSoup(response.content, "html.parser")

provincias = soup.find_all('ul', attrs={"class": "column-list"})
provincias = [i.text.strip() for i in provincias]  # Utilizamos strip() para eliminar los espacios en blanco

provincias = [p.replace("Buenos Aires (CABA)", "Capital Federal") for p in provincias]
provincias = [p.replace("Córdoba", "Cordoba") for p in provincias]
provincias = [p.replace("Entre Ríos", "Entre Rios") for p in provincias]
provincias = [p.replace("Neuquén", "Neuquen") for p in provincias]
provincias = [p.replace("Río Negro", "Rio Negro") for p in provincias]
provincias = [p.replace("Santa Fé", "Santa Fe") for p in provincias]
provincias = [p.replace("Tucumán", "Tucuman") for p in provincias]
provincias = [p.replace(" ", "-") for p in provincias]  # Reemplazamos espacios por guiones ("-")

# Guardar las provincias en un archivo CSV sin comillas
with open('provincias.csv', 'w', encoding='utf-8') as csvfile:
    csvfile.write("Provincia\n")
    for provincia in provincias:
        csvfile.write(f"{provincia}\n")

# Leer las provincias desde el archivo CSV
with open('provincias.csv', 'r', encoding='utf-8') as csvfile:
    next(csvfile)  # Saltar la primera fila (encabezado)
    for row in csvfile:
        provincia = row.strip()
        print(provincia)


# Obtener las provincias desde la página web
url = "https://codigo-postal.co/argentina/"
response = requests.get(url)
soup = BeautifulSoup(response.content, "html.parser")
provincias = soup.find_all('ul', attrs={"class": "column-list"})
provincias = [i.text.strip() for i in provincias] # Utilizamos strip() para eliminar los espacios en blanco
provincias = [p.replace("Buenos Aires (CABA)", "Capital Federal") for p in provincias]
provincias = [p.replace("Córdoba", "Cordoba") for p in provincias]
provincias = [p.replace("Entre Ríos", "Entre Rios") for p in provincias]
provincias = [p.replace("Neuquén", "Neuquen") for p in provincias]
provincias = [p.replace("Río Negro", "Rio Negro") for p in provincias]
provincias = [p.replace("Santa Fé", "Santa Fe") for p in provincias]
provincias = [p.replace("Tucumán", "Tucuman") for p in provincias]
provincias = [p.replace(" ", "-") for p in provincias] # Reemplazamos espacios por guiones ("-")

# Guardar las provincias en un archivo CSV sin comillas
with open('provincias.csv', 'w', encoding='utf-8') as csvfile:
    csvfile.write("Provincia\n")
    for provincia in provincias:
        csvfile.write(f"{provincia}\n")


# Leer las provincias desde el archivo CSV
with open('provincias.csv', 'r', encoding='utf-8') as csvfile:
    next(csvfile) # Saltar la primera fila (encabezado)
    for row in csvfile:
        provincia = row.strip()
        print(provincia)

print('SEGURDARON TODAS LAS PROVINCIAS EN provincias.csv')


# Leer las provincias desde el archivo CSV
provincias = []
with open('provincias.csv', 'r', encoding='utf-8') as csvfile:
    next(csvfile) # Saltar la primera fila (encabezado)
    for row in csvfile:
        provincia = row.strip()
        provincias.append(provincia)

# Crear archivo CSV para guardar los resultados
with open('provincias_cities.csv', 'w', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['PROVINCIA', 'CITY'])
    # Buscar las ciudades para cada provincia
    for provincia in provincias:
        url_provincia = f"https://codigo-postal.co/argentina/{provincia}"
        response_provincia = requests.get(url_provincia)
        soup_provincia = BeautifulSoup(response_provincia.content, "html.parser")
        cities = soup_provincia.find('ul', attrs={"class": "cities"})
        cities = [city.text for city in cities]
        cities = [c.replace(" ", "-") for c in cities]
        # Escribir en el archivo CSV
        if cities:
            for city in cities:
                writer.writerow([provincia, city])
        else:
            writer.writerow([provincia, 'No se encontraron ciudades'])

# Imprimir el contenido del archivo CSV
with open('provincias_cities.csv', 'r', encoding='utf-8') as csvfile:
    for row in csvfile:
        #print(row.strip())
        print(row)

print('SE GUARDARON TODAS LAS PROVINCIAS Y LOCALIDADES DENTRO DE provincias_cities.csv')


prob_cities = pd.read_csv('./provincias_cities.csv')
prob_cities= prob_cities.applymap(lambda x: x.strip() if isinstance(x, str) else x)
prob_cities.to_csv('provincias_cities.csv',index=False)












#PRUEBA

import requests
from bs4 import BeautifulSoup
import csv
import concurrent.futures

# Función para procesar un registro
def procesar_registro(registro):
    provincia, city = registro
    try:
        # Realizar la solicitud HTTP a la página
        url = f"https://codigo-postal.co/argentina/{provincia}/{city}"
        response = requests.get(url, allow_redirects=True)
        soup = BeautifulSoup(response.content, "html.parser")

        # Encontrar los elementos con la ruta "//tbody/tr/td/a"
        CPAs = soup.select("tbody tr td a")
        CPAs_text = [elemento.text for elemento in CPAs]

        return [[provincia, city, cpa, ''] for cpa in CPAs_text]

    except requests.TooManyRedirects:
        return [[provincia, city, '', 'TooManyRedirects']]
    except requests.ConnectionError as e:
        return [[provincia, city, '', str(e)]]

# Leer el archivo CSV completo
provincias_cities = []
with open('provincias_cities.csv', 'r', encoding='utf-8') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)  # Saltar la primera fila (encabezado)
    for row in reader:
        provincia, city = row
        provincias_cities.append((provincia, city))

# Crear archivo CSV para guardar los resultados
with open('provicia_localidades_CPA.csv', 'w', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['PROVINCIA', 'CITY', 'CPA', 'ERROR'])

    # Procesar los registros en paralelo utilizando múltiples hilos
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Mapear cada registro a un hilo y obtener los resultados
        resultados = executor.map(procesar_registro, provincias_cities)

        # Escribir los resultados en el archivo CSV
        for resultado in resultados:
            for r in resultado:
                writer.writerow(r)

print("Los resultados se han guardado en 'provicia_localidades_CPA.csv'.")
# Imprimir los registros filtrados


#Leo nuevamente el csv 
provincia_localidades = pd.read_csv('./provicia_localidades_CPA.csv')
#Quito los espacios en blanco entre cada dato
provincia_localidades = provincia_localidades.applymap(lambda x: x.strip() if isinstance(x, str) else x)
#Vuelvo a guardarlo con las modificaciones
provincia_localidades.to_csv('./provicia_localidades_CPA.csv')
#Leo nuevamente el data set 
filtrado = pd.read_csv('./provicia_localidades_CPA.csv')
# Relleno los valores faltantes con una cadena vacía
filtrado['CPA'] = filtrado['CPA'].fillna('')
# Filtrar los registros que contienen "Buscar CPA" en la columna 'CPA'
filtered_df = filtrado[filtrado['CPA'].str.contains('Buscar CPA')]
# Borro los duplicados
filtered_df = filtered_df.drop_duplicates()
# Defino una función para agregar el prefijo "calles-de-"
def agregar_prefijo(valor):
    return "calles-de-" + valor
# Aplica la función a la columna "CITY" utilizando el método apply
filtered_df["CITY"] = filtered_df["CITY"].apply(agregar_prefijo)
# Paso la informacino a un nuevo csv donde solo esten las provincias donde el CPA  sea "Buscar CPA"
filtered_df.to_csv('./filtrado_sin_cpa.csv')

print('HASTA ACA SON TODAS LAS TRANSFORMACIONES')

# Crear un archivo CSV para guardar los resultados
with open('INTENTO_CALLESYCPANUEVO.csv', 'w', encoding='utf-8', newline='') as outfile:
    writer = csv.writer(outfile)
    writer.writerow(['URL', 'Calle/Avenida', 'desde', 'hasta', 'aplica a', 'Código Postal', 'CPA', 'PROVINCIA'])

    # Leer el archivo CSV
    with open('filtrado_sin_cpa.csv', 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        next(reader)  # Saltar la primera fila (encabezado)

        # Recorrer las filas del archivo CSV
        for row in reader:
            provincia = row['PROVINCIA']
            city = row['CITY']

            # Construir la URL con la provincia y la city
            url = f"https://codigo-postal.co/argentina/{provincia}/{city}/"
            
            try:
                # Realizar la solicitud HTTP a la página
                response = requests.get(url)
                tree = html.fromstring(response.content)

                # Encontrar todos los elementos con el XPath "//ul/li/a"
                elementos = tree.xpath('/html/body/div[3]/div[2]/div[1]/div[3]/div[1]/ul/li/a')

                # Recorrer los elementos encontrados
                for elemento in elementos:
                    # Obtener el enlace y realizar la solicitud HTTP a cada uno de ellos
                    enlace = elemento.get('href')
                    enlace_response = requests.get(enlace)
                    enlace_tree = html.fromstring(enlace_response.content)

                    # Encontrar el elemento con el XPath "/html/body/div[3]/div[2]/div[1]/div[1]/div/table/tbody/tr[1]"
                    resultado = enlace_tree.xpath('/html/body/div[3]/div[2]/div[1]/div[1]/div/table/tbody/tr[1]')

                    # Obtener los valores de cada columna y escribirlos en el archivo CSV
                    if resultado:
                        valores = [city] + [elem.text_content().strip() for elem in resultado[0].xpath('td')] + [provincia]
                        writer.writerow(valores)
            except requests.RequestException as e:
                print(f"Error al procesar la URL: {city}")
                print(e)

print("Los resultados se han guardado en 'INTENTO_CALLESYCPA.csv'.")