# Databricks notebook source
# MAGIC %md
# MAGIC # 03. Data Visualization - Telecomm CHURN Analysis
# MAGIC
# MAGIC **Workshop: Telecomm Customer Churn Analysis**
# MAGIC
# MAGIC This notebook provides comprehensive churn analysis and visualizations for Telecomm's customer data.
# MAGIC
# MAGIC **Table:** `workshop_megacable.{usuario}_gold.clientes`
# MAGIC
# MAGIC **Business Objectives:**
# MAGIC - Identify key factors driving customer churn
# MAGIC - Analyze revenue impact of churn
# MAGIC - Create actionable insights for retention strategies
# MAGIC - Build dashboards for AI/BI and Genie integration

# COMMAND ----------

# MAGIC %run ./setup/SetupWorkshop

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Data Setup and Initial Exploration

# COMMAND ----------

spark.sql(f"USE workshop_megacable.{usuario}_gold")

# COMMAND ----------

# MAGIC %md
# MAGIC Validate schema

# COMMAND ----------

result = spark.sql("SELECT current_schema()")
display(result)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample data preview with key churn fields
# MAGIC SELECT 
# MAGIC   id_cliente,
# MAGIC   genero,
# MAGIC   edad,
# MAGIC   tenencia_meses,
# MAGIC   tipo_contrato,
# MAGIC   cargo_mensual,
# MAGIC   total_ingreso,
# MAGIC   estatus_cliente,
# MAGIC   churn_cliente,
# MAGIC   churn_razon
# MAGIC FROM clientes 
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Overall Churn Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Overall churn distribution
# MAGIC SELECT 
# MAGIC   estatus_cliente,
# MAGIC   COUNT(*) as total_customers,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
# MAGIC FROM clientes
# MAGIC GROUP BY estatus_cliente
# MAGIC ORDER BY total_customers DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Churn reasons analysis
# MAGIC SELECT 
# MAGIC   churn_razon,
# MAGIC   COUNT(*) as churned_customers,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage_of_churn
# MAGIC FROM clientes
# MAGIC WHERE estatus_cliente = 'Churned'
# MAGIC   AND churn_razon IS NOT NULL
# MAGIC GROUP BY churn_razon
# MAGIC ORDER BY churned_customers DESC

# COMMAND ----------

# Import necessary libraries
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Set visualization parameters
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")
plt.rcParams['figure.figsize'] = (12, 6)

# COMMAND ----------

# Load the data
df = spark.table("clientes")

# Basic data overview
print(f"Total customers: {df.count():,}")
print(f"Total columns: {len(df.columns)}")
print("\nTable Schema:")
df.printSchema()

# COMMAND ----------

# Create visualization for churn distribution
churn_summary = spark.sql("""
    SELECT 
        estatus_cliente,
        COUNT(*) as count
    FROM clientes
    GROUP BY estatus_cliente
""").toPandas()

# Plot churn distribution
plt.figure(figsize=(10, 6))
plt.subplot(1, 2, 1)
colors = ['#2E8B57', '#DC143C']  # Green for Stayed, Red for Churned
plt.pie(churn_summary['count'], labels=churn_summary['estatus_cliente'], 
        autopct='%1.1f%%', colors=colors, startangle=90)
plt.title('Customer Churn Distribution')

# Plot count bar chart
plt.subplot(1, 2, 2)
bars = plt.bar(churn_summary['estatus_cliente'], churn_summary['count'], color=colors)
plt.title('Customer Count by Status')
plt.ylabel('Number of Customers')
for bar in bars:
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2., height + 50,
             f'{int(height):,}', ha='center', va='bottom')

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Demographic Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Churn by gender and age groups
# MAGIC SELECT 
# MAGIC   genero,
# MAGIC   CASE 
# MAGIC     WHEN edad < 30 THEN '18-29'
# MAGIC     WHEN edad < 45 THEN '30-44'
# MAGIC     WHEN edad < 60 THEN '45-59'
# MAGIC     ELSE '60+'
# MAGIC   END as grupo_edad,
# MAGIC   COUNT(*) as total_customers,
# MAGIC   SUM(CASE WHEN estatus_cliente = 'Churned' THEN 1 ELSE 0 END) as churned_customers,
# MAGIC   ROUND(SUM(CASE WHEN estatus_cliente = 'Churned' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as churn_rate
# MAGIC FROM clientes
# MAGIC GROUP BY genero, 
# MAGIC   CASE 
# MAGIC     WHEN edad < 30 THEN '18-29'
# MAGIC     WHEN edad < 45 THEN '30-44'
# MAGIC     WHEN edad < 60 THEN '45-59'
# MAGIC     ELSE '60+'
# MAGIC   END
# MAGIC ORDER BY churn_rate DESC

# COMMAND ----------

# Create demographic churn analysis visualization
demo_data = spark.sql("""
    SELECT 
        genero,
        CASE 
            WHEN edad < 30 THEN '18-29'
            WHEN edad < 45 THEN '30-44'
            WHEN edad < 60 THEN '45-59'
            ELSE '60+'
        END as grupo_edad,
        COUNT(*) as total_customers,
        SUM(CASE WHEN estatus_cliente = 'Churned' THEN 1 ELSE 0 END) as churned_customers,
        ROUND(SUM(CASE WHEN estatus_cliente = 'Churned' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as churn_rate
    FROM clientes
    GROUP BY genero, 
        CASE 
            WHEN edad < 30 THEN '18-29'
            WHEN edad < 45 THEN '30-44'
            WHEN edad < 60 THEN '45-59'
            ELSE '60+'
        END
""").toPandas()

# Convert churn_rate to float
demo_data['churn_rate'] = demo_data['churn_rate'].astype(float)

# Create heatmap for churn rates by demographics
pivot_data = demo_data.pivot(index='grupo_edad', columns='genero', values='churn_rate')

plt.figure(figsize=(10, 6))
sns.heatmap(pivot_data, annot=True, fmt='.1f', cmap='Reds', 
            cbar_kws={'label': 'Churn Rate (%)'})
plt.title('Churn Rate by Age Group and Gender')
plt.ylabel('Age Group')
plt.xlabel('Gender')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Service and Contract Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Churn by contract type and tenure
# MAGIC SELECT 
# MAGIC   tipo_contrato,
# MAGIC   CASE 
# MAGIC     WHEN tenencia_meses <= 12 THEN '0-12 months'
# MAGIC     WHEN tenencia_meses <= 24 THEN '13-24 months'
# MAGIC     WHEN tenencia_meses <= 36 THEN '25-36 months'
# MAGIC     WHEN tenencia_meses <= 48 THEN '37-48 months'
# MAGIC     ELSE '48+ months'
# MAGIC   END as tenure_group,
# MAGIC   COUNT(*) as total_customers,
# MAGIC   SUM(CASE WHEN estatus_cliente = 'Churned' THEN 1 ELSE 0 END) as churned_customers,
# MAGIC   ROUND(SUM(CASE WHEN estatus_cliente = 'Churned' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as churn_rate,
# MAGIC   ROUND(AVG(tenencia_meses), 1) as avg_tenure_months
# MAGIC FROM clientes
# MAGIC GROUP BY tipo_contrato, 
# MAGIC   CASE 
# MAGIC     WHEN tenencia_meses <= 12 THEN '0-12 months'
# MAGIC     WHEN tenencia_meses <= 24 THEN '13-24 months'
# MAGIC     WHEN tenencia_meses <= 36 THEN '25-36 months'
# MAGIC     WHEN tenencia_meses <= 48 THEN '37-48 months'
# MAGIC     ELSE '48+ months'
# MAGIC   END
# MAGIC ORDER BY churn_rate DESC

# COMMAND ----------

# Create contract and tenure analysis
contract_data = spark.sql("""
    SELECT 
        tipo_contrato,
        COUNT(*) as total_customers,
        SUM(CASE WHEN estatus_cliente = 'Churned' THEN 1 ELSE 0 END) as churned_customers,
        ROUND(SUM(CASE WHEN estatus_cliente = 'Churned' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as churn_rate
    FROM clientes
    GROUP BY tipo_contrato
    ORDER BY churn_rate DESC
""").toPandas()

plt.figure(figsize=(15, 5))

# Contract type churn rates
plt.subplot(1, 3, 1)
bars = plt.bar(contract_data['tipo_contrato'], contract_data['churn_rate'], 
               color=['#ff7f7f', '#ffb347', '#87ceeb'])
plt.title('Churn Rate by Contract Type')
plt.ylabel('Churn Rate (%)')
plt.xticks(rotation=45)
for bar in bars:
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2., height + 0.5,
             f'{height:.1f}%', ha='center', va='bottom')

# Tenure distribution
tenure_data = spark.sql("""
    SELECT tenencia_meses, estatus_cliente 
    FROM clientes
""").toPandas()

plt.subplot(1, 3, 2)
churned = tenure_data[tenure_data['estatus_cliente'] == 'Churned']['tenencia_meses']
stayed = tenure_data[tenure_data['estatus_cliente'] == 'Stayed']['tenencia_meses']

plt.hist([stayed, churned], bins=20, label=['Stayed', 'Churned'], 
         color=['green', 'red'], alpha=0.7)
plt.title('Tenure Distribution by Churn Status')
plt.xlabel('Tenure (months)')
plt.ylabel('Number of Customers')
plt.legend()

# Service type analysis
service_data = spark.sql("""
    SELECT 
        COALESCE(tipo_internet, 'No Internet') as internet_type,
        COUNT(*) as total_customers,
        SUM(CASE WHEN estatus_cliente = 'Churned' THEN 1 ELSE 0 END) as churned_customers,
        ROUND(SUM(CASE WHEN estatus_cliente = 'Churned' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as churn_rate
    FROM clientes
    GROUP BY COALESCE(tipo_internet, 'No Internet')
    ORDER BY churn_rate DESC
""").toPandas()

plt.subplot(1, 3, 3)
bars = plt.bar(service_data['internet_type'], service_data['churn_rate'], 
               color=['#ff6b6b', '#4ecdc4', '#45b7d1'])
plt.title('Churn Rate by Internet Service Type')
plt.ylabel('Churn Rate (%)')
plt.xticks(rotation=45)
for bar in bars:
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2., height + 0.5,
             f'{height:.1f}%', ha='center', va='bottom')

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Revenue and Financial Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Revenue impact analysis
# MAGIC SELECT 
# MAGIC   CASE 
# MAGIC     WHEN cargo_mensual < 30 THEN 'Budget ($0-30)'
# MAGIC     WHEN cargo_mensual < 60 THEN 'Standard ($30-60)'
# MAGIC     WHEN cargo_mensual < 90 THEN 'Premium ($60-90)'
# MAGIC     ELSE 'Enterprise ($90+)'
# MAGIC   END as revenue_tier,
# MAGIC   COUNT(*) as total_customers,
# MAGIC   SUM(CASE WHEN estatus_cliente = 'Churned' THEN 1 ELSE 0 END) as churned_customers,
# MAGIC   ROUND(SUM(CASE WHEN estatus_cliente = 'Churned' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as churn_rate,
# MAGIC   ROUND(AVG(cargo_mensual), 2) as avg_monthly_charge,
# MAGIC   ROUND(SUM(CASE WHEN estatus_cliente = 'Churned' THEN cargo_mensual ELSE 0 END), 2) as monthly_revenue_at_risk
# MAGIC FROM clientes
# MAGIC GROUP BY 
# MAGIC   CASE 
# MAGIC     WHEN cargo_mensual < 30 THEN 'Budget ($0-30)'
# MAGIC     WHEN cargo_mensual < 60 THEN 'Standard ($30-60)'
# MAGIC     WHEN cargo_mensual < 90 THEN 'Premium ($60-90)'
# MAGIC     ELSE 'Enterprise ($90+)'
# MAGIC   END
# MAGIC ORDER BY churn_rate DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Payment method and churn correlation
# MAGIC SELECT 
# MAGIC   tipo_pago,
# MAGIC   COUNT(*) as total_customers,
# MAGIC   SUM(CASE WHEN estatus_cliente = 'Churned' THEN 1 ELSE 0 END) as churned_customers,
# MAGIC   ROUND(SUM(CASE WHEN estatus_cliente = 'Churned' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as churn_rate,
# MAGIC   ROUND(AVG(cargo_mensual), 2) as avg_monthly_charge
# MAGIC FROM clientes
# MAGIC GROUP BY tipo_pago
# MAGIC ORDER BY churn_rate DESC

# COMMAND ----------

# Revenue analysis visualization
revenue_data = spark.sql("""
    SELECT 
        cargo_mensual,
        total_ingreso,
        estatus_cliente
    FROM clientes
""").toPandas()

plt.figure(figsize=(15, 5))

# Monthly charges distribution
plt.subplot(1, 3, 1)
churned_charges = revenue_data[revenue_data['estatus_cliente'] == 'Churned']['cargo_mensual']
stayed_charges = revenue_data[revenue_data['estatus_cliente'] == 'Stayed']['cargo_mensual']

plt.boxplot([stayed_charges, churned_charges], labels=['Stayed', 'Churned'])
plt.title('Monthly Charges Distribution')
plt.ylabel('Monthly Charge ($)')

# Total revenue distribution
plt.subplot(1, 3, 2)
churned_revenue = revenue_data[revenue_data['estatus_cliente'] == 'Churned']['total_ingreso']
stayed_revenue = revenue_data[revenue_data['estatus_cliente'] == 'Stayed']['total_ingreso']

plt.boxplot([stayed_revenue, churned_revenue], labels=['Stayed', 'Churned'])
plt.title('Total Revenue Distribution')
plt.ylabel('Total Revenue ($)')

# Revenue at risk by tier
revenue_tier_data = spark.sql("""
    SELECT 
        CASE 
            WHEN cargo_mensual < 30 THEN 'Budget'
            WHEN cargo_mensual < 60 THEN 'Standard'
            WHEN cargo_mensual < 90 THEN 'Premium'
            ELSE 'Enterprise'
        END as tier,
        SUM(CASE WHEN estatus_cliente = 'Churned' THEN cargo_mensual ELSE 0 END) as revenue_at_risk
    FROM clientes
    GROUP BY 
        CASE 
            WHEN cargo_mensual < 30 THEN 'Budget'
            WHEN cargo_mensual < 60 THEN 'Standard'
            WHEN cargo_mensual < 90 THEN 'Premium'
            ELSE 'Enterprise'
        END
    ORDER BY revenue_at_risk DESC
""").toPandas()

plt.subplot(1, 3, 3)
bars = plt.bar(revenue_tier_data['tier'], revenue_tier_data['revenue_at_risk'],
               color=['#ff9999', '#66b3ff', '#99ff99', '#ffcc99'])
plt.title('Monthly Revenue at Risk by Tier')
plt.ylabel('Revenue at Risk ($)')
plt.xticks(rotation=45)
for bar in bars:
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2., height + 100,
             f'${height:,.0f}', ha='center', va='bottom')

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Geographic and Service Feature Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Geographic churn analysis (top cities by churn count)
# MAGIC SELECT 
# MAGIC   ciudad,
# MAGIC   COUNT(*) as total_customers,
# MAGIC   SUM(CASE WHEN estatus_cliente = 'Churned' THEN 1 ELSE 0 END) as churned_customers,
# MAGIC   ROUND(SUM(CASE WHEN estatus_cliente = 'Churned' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as churn_rate,
# MAGIC   AVG(poblacion) as avg_population
# MAGIC FROM clientes
# MAGIC GROUP BY ciudad
# MAGIC HAVING COUNT(*) >= 2  -- Only cities with multiple customers
# MAGIC ORDER BY churned_customers DESC
# MAGIC LIMIT 15

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Service features impact on churn
# MAGIC SELECT 
# MAGIC   'Phone Service' as feature,
# MAGIC   servicio_telefono as has_feature,
# MAGIC   COUNT(*) as total_customers,
# MAGIC   SUM(CASE WHEN estatus_cliente = 'Churned' THEN 1 ELSE 0 END) as churned_customers,
# MAGIC   ROUND(SUM(CASE WHEN estatus_cliente = 'Churned' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as churn_rate
# MAGIC FROM clientes
# MAGIC GROUP BY servicio_telefono
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Internet Service' as feature,
# MAGIC   servicio_internet as has_feature,
# MAGIC   COUNT(*) as total_customers,
# MAGIC   SUM(CASE WHEN estatus_cliente = 'Churned' THEN 1 ELSE 0 END) as churned_customers,
# MAGIC   ROUND(SUM(CASE WHEN estatus_cliente = 'Churned' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as churn_rate
# MAGIC FROM clientes
# MAGIC GROUP BY servicio_internet
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Online Security' as feature,
# MAGIC   seguridad_online as has_feature,
# MAGIC   COUNT(*) as total_customers,
# MAGIC   SUM(CASE WHEN estatus_cliente = 'Churned' THEN 1 ELSE 0 END) as churned_customers,
# MAGIC   ROUND(SUM(CASE WHEN estatus_cliente = 'Churned' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as churn_rate
# MAGIC FROM clientes
# MAGIC GROUP BY seguridad_online
# MAGIC
# MAGIC ORDER BY churn_rate DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Customer Lifetime Value and Risk Segmentation

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Customer segmentation for retention strategies
# MAGIC SELECT 
# MAGIC   CASE 
# MAGIC     WHEN tenencia_meses <= 6 AND cargo_mensual > 70 THEN 'High Value New Customer'
# MAGIC     WHEN tenencia_meses <= 12 AND tipo_contrato = 'Month-to-Month' THEN 'At Risk New Customer'
# MAGIC     WHEN tenencia_meses > 36 AND cargo_mensual > 80 THEN 'High Value Loyal Customer'
# MAGIC     WHEN tenencia_meses > 24 AND estatus_cliente = 'Stayed' THEN 'Stable Long-term Customer'
# MAGIC     WHEN cargo_mensual < 30 THEN 'Budget Customer'
# MAGIC     ELSE 'Standard Customer'
# MAGIC   END as customer_segment,
# MAGIC   COUNT(*) as total_customers,
# MAGIC   SUM(CASE WHEN estatus_cliente = 'Churned' THEN 1 ELSE 0 END) as churned_customers,
# MAGIC   ROUND(SUM(CASE WHEN estatus_cliente = 'Churned' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as churn_rate,
# MAGIC   ROUND(AVG(cargo_mensual), 2) as avg_monthly_revenue,
# MAGIC   ROUND(AVG(total_ingreso), 2) as avg_total_revenue,
# MAGIC   ROUND(SUM(CASE WHEN estatus_cliente = 'Churned' THEN total_ingreso ELSE 0 END), 2) as total_revenue_lost
# MAGIC FROM clientes
# MAGIC GROUP BY 
# MAGIC   CASE 
# MAGIC     WHEN tenencia_meses <= 6 AND cargo_mensual > 70 THEN 'High Value New Customer'
# MAGIC     WHEN tenencia_meses <= 12 AND tipo_contrato = 'Month-to-Month' THEN 'At Risk New Customer'
# MAGIC     WHEN tenencia_meses > 36 AND cargo_mensual > 80 THEN 'High Value Loyal Customer'
# MAGIC     WHEN tenencia_meses > 24 AND estatus_cliente = 'Stayed' THEN 'Stable Long-term Customer'
# MAGIC     WHEN cargo_mensual < 30 THEN 'Budget Customer'
# MAGIC     ELSE 'Standard Customer'
# MAGIC   END
# MAGIC ORDER BY churn_rate DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Key Metrics Dashboard

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Executive Dashboard KPIs
# MAGIC SELECT 
# MAGIC   'Total Customers' as metric,
# MAGIC   FORMAT_NUMBER(COUNT(*), 0) as value,
# MAGIC   'customers' as unit
# MAGIC FROM clientes
# MAGIC   
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Churned Customers' as metric,
# MAGIC   FORMAT_NUMBER(SUM(CASE WHEN estatus_cliente = 'Churned' THEN 1 ELSE 0 END), 0) as value,
# MAGIC   'customers' as unit
# MAGIC FROM clientes
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Overall Churn Rate' as metric,
# MAGIC   CONCAT(ROUND(SUM(CASE WHEN estatus_cliente = 'Churned' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1), '%') as value,
# MAGIC   'percentage' as unit
# MAGIC FROM clientes
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Monthly Revenue at Risk' as metric,
# MAGIC   CONCAT('$', FORMAT_NUMBER(SUM(CASE WHEN estatus_cliente = 'Churned' THEN cargo_mensual ELSE 0 END), 2)) as value,
# MAGIC   'dollars' as unit
# MAGIC FROM clientes
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Average Customer Tenure' as metric,
# MAGIC   CONCAT(ROUND(AVG(tenencia_meses), 1), ' months') as value,
# MAGIC   'months' as unit
# MAGIC FROM clientes
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Average Monthly Revenue' as metric,
# MAGIC   CONCAT('$', FORMAT_NUMBER(AVG(cargo_mensual), 2)) as value,
# MAGIC   'dollars' as unit
# MAGIC FROM clientes

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Dashboard Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create optimized view for Genie natural language queries
# MAGIC CREATE OR REPLACE VIEW workshop_megacable.analysis.churn_analytics_materialized_view AS
# MAGIC SELECT 
# MAGIC id_cliente,
# MAGIC genero,
# MAGIC edad,
# MAGIC CASE 
# MAGIC   WHEN edad < 30 THEN 'Joven (18-29)'
# MAGIC   WHEN edad < 45 THEN 'Adulto (30-44)'
# MAGIC   WHEN edad < 60 THEN 'Maduro (45-59)'
# MAGIC   ELSE 'Senior (60+)'
# MAGIC END as grupo_edad,
# MAGIC casado,
# MAGIC dependientes,
# MAGIC ciudad,
# MAGIC codigo_postal,
# MAGIC tenencia_meses,
# MAGIC CASE 
# MAGIC   WHEN tenencia_meses <= 12 THEN 'Nuevo (0-12 meses)'
# MAGIC   WHEN tenencia_meses <= 36 THEN 'Establecido (13-36 meses)'
# MAGIC   ELSE 'Leal (36+ meses)'
# MAGIC END as categoria_tenencia,
# MAGIC tipo_contrato,
# MAGIC tipo_pago,
# MAGIC servicio_telefono,
# MAGIC servicio_internet,
# MAGIC tipo_internet,
# MAGIC seguridad_online,
# MAGIC backup_online,
# MAGIC soporte_premium,
# MAGIC tv_streaming,
# MAGIC cargo_mensual,
# MAGIC CASE 
# MAGIC   WHEN cargo_mensual < 30 THEN 'Económico'
# MAGIC   WHEN cargo_mensual < 60 THEN 'Estándar'
# MAGIC   WHEN cargo_mensual < 90 THEN 'Premium'
# MAGIC   ELSE 'Empresarial'
# MAGIC END as nivel_ingresos,
# MAGIC total_ingreso,
# MAGIC estatus_cliente,
# MAGIC CASE WHEN estatus_cliente = 'Churned' THEN 1 ELSE 0 END as es_churn,
# MAGIC churn_razon,
# MAGIC CASE 
# MAGIC   WHEN tenencia_meses <= 6 AND cargo_mensual > 70 THEN 'Alto Valor Nuevo'
# MAGIC   WHEN tenencia_meses <= 12 AND tipo_contrato = 'Month-to-Month' THEN 'Nuevo En Riesgo'
# MAGIC   WHEN tenencia_meses > 36 AND cargo_mensual > 80 THEN 'Alto Valor Leal'
# MAGIC   WHEN tenencia_meses > 24 THEN 'Estable Largo Plazo'
# MAGIC   WHEN cargo_mensual < 30 THEN 'Económico'
# MAGIC   ELSE 'Estándar'
# MAGIC END as segmento_cliente
# MAGIC FROM clientes
