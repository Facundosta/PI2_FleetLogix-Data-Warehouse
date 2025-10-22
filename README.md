# FleetLogix - Data Warehouse & Analytics Platform

Sistema integral de gestión de flotas vehiculares y Data Warehouse diseñado específicamente para operaciones logísticas en Argentina.

## Características Principales

### Arquitectura de Datos
- **Base de datos transaccional**: PostgreSQL con 506,650+ registros sintéticos
- **Data Warehouse**: Snowflake con modelo estrella optimizado
- **Pipeline ETL**: Automatizado en Python con validación multi-nivel
- **Arquitectura cloud**: AWS (RDS, DynamoDB, Lambda, S3, API Gateway)

### Métricas Clave
- **506,650+ registros** generados sintéticamente
- **ETL performance**: 48-49 segundos (proceso completo)
- **12 queries optimizadas** con índices estratégicos
- **32.9% entregas a tiempo** - análisis de eficiencia operativa

## Estructura del Proyecto
```bash
ProyectoM2_FacundoAcosta/
├── Scripts/
│ ├── 01_data_generation/ # Generación de 500k+ registros
│ ├── 02_y_03_queries_optimization/ # 12 queries + optimización
│ ├── 04_dimensional_model/ # DDL Data Warehouse
│ ├── 05_etl_pipeline/ # Pipeline ETL completo
│ └── 06_aws_setup/ # Arquitectura AWS + Lambda
├── Documentación/
│ ├── FA_README.pdf
│ ├── FA_Fleetlogix_ER_Diagram.JPG
│ ├── FA_Análisis_del_modelo_proporcionado.pdf
│ ├── FA_Manual_Consultas_SQL.pdf
│ ├── FA_Análisis_Snowflake_ETL.pdf
│ ├── FA_aws_architecture_diagram.png
│ └── FA_AMS_Análisis_Arquitectura.pdf
└── README.md
```


## Tecnologías Implementadas

- **Backend**: Python 3.8+, SQLAlchemy, Pandas, Faker
- **Bases de datos**: PostgreSQL, Snowflake, DynamoDB
- **Cloud**: AWS (RDS, Lambda, S3, API Gateway)
- **Metodologías**: ETL, Data Modeling, Query Optimization

## Autor

**Facundo Acosta** - Data Scientist  
📧 facundoacostast@gmail.com  
💼 [LinkedIn](https://linkedin.com/in/facundo-acosta-marketing/)
