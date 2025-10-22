"""
Script de Transformación de Datos para FleetLogix Data Warehouse - ADAPTADO
Autor: Facundo Acosta
Descripción: Aplica transformaciones y validaciones a los datos extraídos de PostgreSQL
Version: 2.0 - Adaptado para estructura de datos actual
FUNCIONALIDADES PRINCIPALES:
1. Transformación de datos brutos a modelo dimensional
2. Cálculo de métricas de negocio y eficiencia  
3. Validación de calidad de datos
4. Preparación para carga en Snowflake
"""

import pandas as pd
import numpy as np
from datetime import datetime
import warnings

# Suprimir warnings de pandas
warnings.filterwarnings('ignore', category=pd.errors.SettingWithCopyWarning)

def transform_delivery_data(raw_df):
    """Aplicar transformaciones a los datos de entrega - ADAPTADO"""
    if raw_df.empty:
        print("DataFrame vacío, no hay datos para transformar")
        return raw_df
    
    df = raw_df.copy()
    print(f"Iniciando transformación de {len(df):,} registros")
    
    try:
        # 1. Calcular duración de entrega en minutos (ADAPTADO)
        print("  Calculando duración de entrega...")
        df['delivery_duration_minutes'] = (
            pd.to_datetime(df['delivered_datetime']) - 
            pd.to_datetime(df['scheduled_datetime'])
        ).dt.total_seconds() / 60
        
        # 2. Calcular delay en minutos (NUEVO)
        print("  Calculando delay de entrega...")
        df['delay_minutes_calculated'] = (
            pd.to_datetime(df['delivered_datetime']) - 
            pd.to_datetime(df['scheduled_datetime'])
        ).dt.total_seconds() / 60
        
        # 3. Determinar si la entrega fue a tiempo (usando la lógica existente)
        print("  Evaluando puntualidad...")
        df['on_time_status'] = df['delay_minutes'] <= 0  # 0 o negativo = a tiempo
        
        # 4. Calcular eficiencia de combustible (km/litro) - ADAPTADO
        print("  Calculando eficiencia de combustible...")
        df['fuel_efficiency_calculated'] = np.where(
            df['delivery_fuel_consumed'] > 0,
            df['delivery_distance_km'] / df['delivery_fuel_consumed'],
            0
        )
        
        # 5. Calcular ingresos basados en métricas existentes - ADAPTADO
        print("  Calculando métricas de ingresos...")
        df['revenue_per_delivery_calculated'] = np.where(
            df['delivery_status'] == 'delivered',
            df['revenue_per_delivery'],
            0
        )
        
        # 6. Aplicar validaciones de calidad de datos
        print("  Validando calidad de datos...")
        df = validate_data_quality(df)
        
        # 7. Verificar y completar claves dimensionales (ADAPTADO)
        print("  Verificando claves dimensionales...")
        df = verify_dimension_keys(df)
        
        # 8. Agregar metadatos de transformación
        df['transformation_timestamp'] = datetime.now()
        df['data_quality_score'] = calculate_quality_score(df)
        
        print(f"Transformación completada: {len(df):,} registros válidos")
        return df
        
    except Exception as e:
        print(f"Error en transformación: {e}")
        raise

def validate_data_quality(df):
    """Validar y limpiar la calidad de los datos - ADAPTADO"""
    initial_count = len(df)
    
    # Crear máscara de validación combinada (ADAPTADA para nueva estructura)
    valid_mask = (
        (df['delivery_duration_minutes'] > 0) &
        (df['delivery_duration_minutes'] < 1440) &  # Máximo 24 horas
        (df['delivery_distance_km'] > 0) &
        (df['delivery_distance_km'] < 5000) &  # Máximo 5000 km
        (df['package_weight_kg'] >= 0) &
        (df['package_weight_kg'] < 10000) &  # Máximo razonable
        (df['fuel_efficiency_km_per_liter'] > 0) &
        (df['fuel_efficiency_km_per_liter'] < 50) &  # Máximo realista: 50 km/lt
        (pd.to_datetime(df['delivered_datetime']) >= pd.to_datetime(df['scheduled_datetime'])) &
        (df['delivery_status'].notna()) &
        (df['vehicle_id'].notna()) &
        (df['driver_id'].notna())
    )
    
    # Aplicar máscara
    df_clean = df[valid_mask].copy()
    
    removed_count = initial_count - len(df_clean)
    if removed_count > 0:
        print(f"    Removidas {removed_count:,} filas por problemas de calidad ({removed_count/initial_count*100:.1f}%)")
    else:
        print(f"    Todos los registros pasaron validación")
    
    return df_clean

def verify_dimension_keys(df):
    """
    Verificar y completar claves para las 6 dimensiones - ADAPTADO
    Las claves ya vienen generadas desde extract.py, solo verificamos
    """
    print("    Verificando claves dimensionales existentes...")
    
    # Lista de claves dimensionales que YA DEBERÍAN EXISTIR desde extract.py
    expected_keys = {
        'date_key': 'Fecha (YYYYMMDD)',
        'scheduled_time_key': 'Hora programada (HHMM)', 
        'delivered_time_key': 'Hora entregada (HHMM)',
        'vehicle_id': 'ID Vehículo',
        'driver_id': 'ID Conductor',
        'route_id': 'ID Ruta', 
        'customer_id': 'ID Cliente'
    }
    
    # Verificar presencia de claves
    missing_keys = [key for key in expected_keys.keys() if key not in df.columns]
    existing_keys = [key for key in expected_keys.keys() if key in df.columns]
    
    if missing_keys:
        print(f"    ⚠️ Claves faltantes: {missing_keys}")
    else:
        print(f"    ✅ Todas las claves dimensionales presentes")
    
    # Mostrar rangos de valores para claves existentes
    for key in existing_keys:
        if key in df.columns:
            unique_vals = df[key].nunique()
            min_val = df[key].min()
            max_val = df[key].max()
            print(f"       {key:20} {unique_vals:>6} únicos, rango: {min_val} - {max_val}")
    
    return df

def calculate_quality_score(df):
    """Calcular score de calidad de datos (0-100) - ADAPTADO"""
    score = 100
    
    # Penalizar valores nulos
    null_penalty = (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 50
    score -= null_penalty
    
    # Penalizar valores extremos en eficiencia de combustible
    if 'fuel_efficiency_km_per_liter' in df.columns:
        extreme_efficiency = ((df['fuel_efficiency_km_per_liter'] < 5) | 
                             (df['fuel_efficiency_km_per_liter'] > 30)).sum()
        score -= (extreme_efficiency / len(df)) * 20
    
    # Penalizar duraciones extremas
    if 'delivery_duration_minutes' in df.columns:
        extreme_duration = ((df['delivery_duration_minutes'] < 30) | 
                           (df['delivery_duration_minutes'] > 600)).sum()
        score -= (extreme_duration / len(df)) * 15
    
    # Penalizar entregas con mucho delay
    if 'delay_minutes' in df.columns:
        extreme_delay = (df['delay_minutes'] > 240).sum()  # Más de 4 horas de delay
        score -= (extreme_delay / len(df)) * 10
    
    return max(0, min(100, score))

def get_final_columns():
    """Definir columnas finales para la tabla FACT_DELIVERIES en Snowflake - ADAPTADO"""
    return [
        # 7 CLAVES DIMENSIONALES (REQUERIDAS) - ADAPTADO
        'date_key', 
        'scheduled_time_key',
        'delivered_time_key',
        'vehicle_id',  # Usamos los IDs originales como claves
        'driver_id', 
        'route_id', 
        'customer_id',
        
        # IDENTIFICADORES
        'delivery_id',
        'trip_id',
        'tracking_number',
        
        # MÉTRICAS PRINCIPALES
        'delivery_duration_minutes',
        'delay_minutes',
        'delivery_distance_km',
        'delivery_fuel_consumed',
        'package_weight_kg',
        'fuel_efficiency_km_per_liter',
        
        # MÉTRICAS DE NEGOCIO
        'revenue_per_delivery',
        'cost_per_delivery', 
        'deliveries_per_hour',
        
        # INDICADORES BOOLEANOS
        'is_on_time',
        'is_damaged',
        'has_signature',
        
        # ESTADOS
        'delivery_status',
        'trip_status'
    ]

def analyze_transformed_data(df):
    """Generar análisis básico de los datos transformados - ADAPTADO"""
    if df.empty:
        print("No hay datos para analizar")
        return
    
    print("\n" + "=" * 70)
    print("ANÁLISIS DE DATOS TRANSFORMADOS")
    print("=" * 70)
    
    # Métricas generales
    print(f"\nMétricas Generales:")
    print(f"   Total de entregas:     {len(df):,}")
    print(f"   Entregas a tiempo:     {df['is_on_time'].sum():,} ({df['is_on_time'].mean()*100:.1f}%)")
    print(f"   Score de calidad:      {df['data_quality_score'].mean():.1f}/100")
    
    # Análisis por dimensiones
    print(f"\nDimensiones:")
    dimension_keys = ['date_key', 'scheduled_time_key', 'delivered_time_key', 
                     'vehicle_id', 'driver_id', 'route_id', 'customer_id']
    for key in dimension_keys:
        if key in df.columns:
            print(f"   {key:20} {df[key].nunique():>6,} valores únicos")
    
    # Métricas de distancia y tiempo
    print(f"\nDistancia y Tiempo:")
    print(f"   Distancia promedio:    {df['delivery_distance_km'].mean():.1f} km")
    print(f"   Distancia total:       {df['delivery_distance_km'].sum():,.1f} km")
    print(f"   Duración promedio:     {df['delivery_duration_minutes'].mean():.1f} min")
    print(f"   Delay promedio:        {df['delay_minutes'].mean():.1f} min")
    
    # Métricas de combustible
    print(f"\nCombustible:")
    print(f"   Consumo promedio:      {df['delivery_fuel_consumed'].mean():.1f} L")
    print(f"   Consumo total:         {df['delivery_fuel_consumed'].sum():,.1f} L")
    print(f"   Eficiencia promedio:   {df['fuel_efficiency_km_per_liter'].mean():.1f} km/L")
    
    # Métricas de negocio
    print(f"\nNegocio:")
    print(f"   Peso total entregado:  {df['package_weight_kg'].sum():,.1f} kg")
    print(f"   Ingreso total:         ${df['revenue_per_delivery'].sum():,.2f}")
    print(f"   Costo total:           ${df['cost_per_delivery'].sum():,.2f}")
    print(f"   Rentabilidad:          ${(df['revenue_per_delivery'] - df['cost_per_delivery']).sum():,.2f}")
    
    # Análisis de estados
    print(f"\nEstados:")
    if 'delivery_status' in df.columns:
        status_counts = df['delivery_status'].value_counts()
        for status, count in status_counts.items():
            print(f"   {status:20} {count:>6,} ({count/len(df)*100:.1f}%)")
    
    print("=" * 70)

def prepare_for_snowflake(df):
    """
    Preparar datos para carga en Snowflake - ADAPTADO
    """
    final_cols = get_final_columns()
    
    # Verificar columnas faltantes
    missing_cols = [col for col in final_cols if col not in df.columns]
    if missing_cols:
        print(f"⚠️ Columnas faltantes para Snowflake: {missing_cols}")
        print(f"   Columnas disponibles: {list(df.columns)}")
        
        # Usar solo las columnas disponibles
        available_cols = [col for col in final_cols if col in df.columns]
        df_final = df[available_cols].copy()
        print(f"   Usando {len(available_cols)} columnas disponibles de {len(final_cols)} esperadas")
    else:
        df_final = df[final_cols].copy()
    
    # MANTENER valores booleanos como están (Snowflake los acepta)
    # No es necesario convertir a 1/0
    
    # Redondear valores numéricos (excepto claves dimensionales y booleanos)
    dimension_keys = ['date_key', 'scheduled_time_key', 'delivered_time_key', 
                     'vehicle_id', 'driver_id', 'route_id', 'customer_id',
                     'delivery_id', 'trip_id']
    
    numeric_cols = [col for col in df_final.select_dtypes(include=[np.number]).columns 
                   if col not in dimension_keys]
    
    for col in numeric_cols:
        df_final[col] = df_final[col].round(2)
    
    # Convertir columnas a MAYÚSCULAS para Snowflake
    df_final.columns = df_final.columns.str.upper()
    
    print(f"✅ Datos preparados para Snowflake: {len(df_final):,} registros, {len(df_final.columns)} columnas")
    print(f"📊 Columnas finales: {', '.join(df_final.columns.tolist())}")
    
    return df_final

def validate_snowflake_compatibility(df):
    """Validar que los datos son compatibles con la estructura de Snowflake - ADAPTADA"""
    print("\nValidando compatibilidad con Snowflake...")
    
    # 1. Verificar columnas requeridas (más flexibles)
    required_columns = get_final_columns()
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print(f"⚠️ Columnas faltantes: {missing_columns}")
        print("   Continuando con columnas disponibles...")
    
    # 2. Verificar que no hay valores nulos en claves dimensionales críticas
    critical_keys = ['delivery_id', 'vehicle_id', 'driver_id', 'date_key']
    critical_keys_present = [key for key in critical_keys if key in df.columns]
    
    if critical_keys_present:
        null_counts = df[critical_keys_present].isnull().sum()
        
        if null_counts.sum() > 0:
            print(f"❌ Valores nulos en claves críticas:")
            for key, count in null_counts[null_counts > 0].items():
                print(f"   - {key}: {count} nulos")
            return False
    
    # 3. Verificar tipos de datos (más flexible)
    if 'is_on_time' in df.columns and df['is_on_time'].dtype not in ['bool', 'int64', 'int32']:
        print(f"❌ is_on_time tiene tipo inválido: {df['is_on_time'].dtype}")
        return False
    
    # 4. Verificar métricas principales
    metrics = ['delivery_duration_minutes', 'delivery_distance_km', 'delivery_fuel_consumed']
    for metric in metrics:
        if metric in df.columns:
            if df[metric].isnull().sum() > 0:
                print(f"⚠️ Valores nulos en {metric}: {df[metric].isnull().sum()}")
            if df[metric].min() < 0:
                print(f"⚠️ Valores negativos en {metric}: mínimo {df[metric].min()}")
    
    # 5. Verificar rangos de time_key (HHMM)
    time_keys = ['scheduled_time_key', 'delivered_time_key']
    for time_key in time_keys:
        if time_key in df.columns:
            if (df[time_key].min() < 0) or (df[time_key].max() > 2359):
                print(f"⚠️ {time_key} fuera de rango: {df[time_key].min()} - {df[time_key].max()}")
    
    print("✅ Datos compatibles con estructura de Snowflake")
    return True

def transform_complete_pipeline(raw_df):
    """
    Pipeline completo de transformación - NUEVA FUNCIÓN
    Combina todas las transformaciones en un flujo
    """
    print("🚀 Iniciando pipeline completo de transformación...")
    
    # 1. Transformación de datos
    transformed_data = transform_delivery_data(raw_df)
    
    if transformed_data.empty:
        print("❌ No hay datos después de la transformación")
        return pd.DataFrame()
    
    # 2. Análisis de datos transformados
    analyze_transformed_data(transformed_data)
    
    # 3. Validar compatibilidad con Snowflake
    snowflake_compatible = validate_snowflake_compatibility(transformed_data)
    
    if not snowflake_compatible:
        print("❌ Los datos no son compatibles con Snowflake")
        return pd.DataFrame()
    
    # 4. Preparar para Snowflake
    snowflake_ready = prepare_for_snowflake(transformed_data)
    
    if snowflake_ready.empty:
        print("❌ Error preparando datos para Snowflake")
        return pd.DataFrame()
    
    print("✅ Pipeline de transformación completado exitosamente")
    return snowflake_ready

# Prueba del módulo adaptado
if __name__ == "__main__":
    # Para probar con datos reales, necesitaríamos importar el extraction
    print("=" * 70)
    print("PRUEBA DE TRANSFORMACIÓN ADAPTADA")
    print("=" * 70)
    
    # Crear datos de prueba con la estructura actual
    sample_data = pd.DataFrame({
        'delivery_id': [1, 2, 3],
        'trip_id': [100, 101, 102],
        'vehicle_id': [201, 202, 203],
        'driver_id': [301, 302, 303],
        'route_id': [401, 402, 403],
        'customer_id': [501, 502, 503],
        'date_key': [20251008, 20251008, 20251008],
        'scheduled_time_key': [900, 930, 1000],
        'delivered_time_key': [915, 945, 1015],
        'scheduled_datetime': ['2025-10-08 09:00:00', '2025-10-08 09:30:00', '2025-10-08 10:00:00'],
        'delivered_datetime': ['2025-10-08 09:15:00', '2025-10-08 09:45:00', '2025-10-08 10:15:00'],
        'delivery_distance_km': [15.5, 20.3, 12.7],
        'delivery_fuel_consumed': [1.2, 1.5, 1.0],
        'package_weight_kg': [5.5, 7.2, 4.8],
        'fuel_efficiency_km_per_liter': [12.9, 13.5, 12.7],
        'delay_minutes': [15, 15, 15],
        'revenue_per_delivery': [25.0, 30.0, 22.5],
        'cost_per_delivery': [18.0, 22.0, 16.5],
        'is_on_time': [False, False, False],
        'is_damaged': [False, False, False],
        'has_signature': [True, True, True],
        'delivery_status': ['delivered', 'delivered', 'delivered'],
        'trip_status': ['completed', 'completed', 'completed']
    })
    
    print(f"Datos de prueba creados: {len(sample_data)} registros")
    
    # Probar transformación
    try:
        result = transform_complete_pipeline(sample_data)
        if not result.empty:
            print(f"\n✅ Transformación exitosa: {len(result)} registros listos para Snowflake")
            print(f"Columnas resultantes: {list(result.columns)}")
    except Exception as e:
        print(f"❌ Error en transformación: {e}")