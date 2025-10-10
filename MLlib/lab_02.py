# ========================================
# Example 10: Model Comparison
# ========================================

# This example shows how to quickly compare different ML algorithms
# to find the best one for your data

print("🏆 Comparing Different ML Algorithms...")

# Import different regression algorithms
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, DecisionTreeRegressor, GBTRegressor
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
import time

print("\n🤖 Available Regression Algorithms in Spark:")
print("   1️⃣ Linear Regression - Simple, fast, interpretable")
print("   2️⃣ Decision Tree - Non-linear, handles interactions")
print("   3️⃣ Random Forest - Multiple trees, robust, accurate")
print("   4️⃣ Gradient Boosted Trees - Sequential learning, very accurate")

# Prepare the data (using our familiar house dataset)
print("\n📊 Preparing data for comparison...")

# Create sample data (or use loaded CSV data)
if 'data' not in locals():
    data = spark.createDataFrame([
        (1500, 2, 1, 10, 300000.0),
        (2000, 3, 2, 5, 450000.0),
        (1200, 1, 1, 15, 250000.0),
        (2500, 4, 3, 2, 600000.0),
        (1800, 2, 2, 8, 380000.0),
        (1600, 2, 1, 12, 320000.0),
        (2200, 3, 2, 6, 480000.0),
        (1000, 1, 1, 20, 200000.0),
        (2300, 4, 3, 3, 550000.0),
        (1700, 3, 2, 9, 390000.0),
        (2600, 4, 3, 1, 650000.0),
        (1300, 2, 1, 18, 280000.0)
    ], ["area", "bedrooms", "bathrooms", "age", "price"])

# Split data once for fair comparison
train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)
print(f"   Training samples: {train_data.count()}")
print(f"   Test samples: {test_data.count()}")

# Feature engineering (same for all models)
assembler = VectorAssembler(
    inputCols=["area", "bedrooms", "bathrooms", "age"],
    outputCol="features"
)

# Create evaluation metrics
rmse_evaluator = RegressionEvaluator(labelCol="price", predictionCol="prediction", metricName="rmse")
r2_evaluator = RegressionEvaluator(labelCol="price", predictionCol="prediction", metricName="r2")
mae_evaluator = RegressionEvaluator(labelCol="price", predictionCol="prediction", metricName="mae")

print("\n🔄 Training and evaluating all models...")

# Store results for comparison
results = []

# 1. LINEAR REGRESSION
print("\n1️⃣ Training Linear Regression...")
start_time = time.time()

lr = LinearRegression(featuresCol="features", labelCol="price")
lr_pipeline = Pipeline(stages=[assembler, lr])
lr_model = lr_pipeline.fit(train_data)
lr_predictions = lr_model.transform(test_data)

lr_rmse = rmse_evaluator.evaluate(lr_predictions)
lr_r2 = r2_evaluator.evaluate(lr_predictions)
lr_mae = mae_evaluator.evaluate(lr_predictions)
lr_time = time.time() - start_time

results.append({
    'algorithm': 'Linear Regression',
    'rmse': lr_rmse,
    'r2': lr_r2,
    'mae': lr_mae,
    'training_time': lr_time,
    'pros': 'Fast, interpretable, works well with linear relationships',
    'cons': 'Cannot capture complex non-linear patterns'
})

print(f"   ✅ RMSE: ${lr_rmse:,.2f}, R²: {lr_r2:.3f}, Time: {lr_time:.2f}s")

# 2. DECISION TREE
print("\n2️⃣ Training Decision Tree...")
start_time = time.time()

dt = DecisionTreeRegressor(featuresCol="features", labelCol="price")
dt_pipeline = Pipeline(stages=[assembler, dt])
dt_model = dt_pipeline.fit(train_data)
dt_predictions = dt_model.transform(test_data)

dt_rmse = rmse_evaluator.evaluate(dt_predictions)
dt_r2 = r2_evaluator.evaluate(dt_predictions)
dt_mae = mae_evaluator.evaluate(dt_predictions)
dt_time = time.time() - start_time

results.append({
    'algorithm': 'Decision Tree',
    'rmse': dt_rmse,
    'r2': dt_r2,
    'mae': dt_mae,
    'training_time': dt_time,
    'pros': 'Handles non-linear patterns, feature interactions',
    'cons': 'Can overfit, less stable than ensemble methods'
})

print(f"   ✅ RMSE: ${dt_rmse:,.2f}, R²: {dt_r2:.3f}, Time: {dt_time:.2f}s")

# 3. RANDOM FOREST
print("\n3️⃣ Training Random Forest...")
start_time = time.time()

rf = RandomForestRegressor(featuresCol="features", labelCol="price", numTrees=10)
rf_pipeline = Pipeline(stages=[assembler, rf])
rf_model = rf_pipeline.fit(train_data)
rf_predictions = rf_model.transform(test_data)

rf_rmse = rmse_evaluator.evaluate(rf_predictions)
rf_r2 = r2_evaluator.evaluate(rf_predictions)
rf_mae = mae_evaluator.evaluate(rf_predictions)
rf_time = time.time() - start_time

results.append({
    'algorithm': 'Random Forest',
    'rmse': rf_rmse,
    'r2': rf_r2,
    'mae': rf_mae,
    'training_time': rf_time,
    'pros': 'Robust, handles overfitting, feature importance',
    'cons': 'Less interpretable, slower than linear models'
})

print(f"   ✅ RMSE: ${rf_rmse:,.2f}, R²: {rf_r2:.3f}, Time: {rf_time:.2f}s")

# 4. GRADIENT BOOSTED TREES
print("\n4️⃣ Training Gradient Boosted Trees...")
start_time = time.time()

gbt = GBTRegressor(featuresCol="features", labelCol="price", maxIter=10)
gbt_pipeline = Pipeline(stages=[assembler, gbt])
gbt_model = gbt_pipeline.fit(train_data)
gbt_predictions = gbt_model.transform(test_data)

gbt_rmse = rmse_evaluator.evaluate(gbt_predictions)
gbt_r2 = r2_evaluator.evaluate(gbt_predictions)
gbt_mae = mae_evaluator.evaluate(gbt_predictions)
gbt_time = time.time() - start_time

results.append({
    'algorithm': 'Gradient Boosted Trees',
    'rmse': gbt_rmse,
    'r2': gbt_r2,
    'mae': gbt_mae,
    'training_time': gbt_time,
    'pros': 'Often highest accuracy, handles complex patterns',
    'cons': 'Slower training, can overfit, less interpretable'
})

print(f"   ✅ RMSE: ${gbt_rmse:,.2f}, R²: {gbt_r2:.3f}, Time: {gbt_time:.2f}s")

# COMPARISON TABLE
print("\n📊 MODEL COMPARISON RESULTS:")
print("=" * 80)
print(f"{'Algorithm':<20} {'RMSE':<12} {'R²':<8} {'MAE':<12} {'Time(s)':<8}")
print("=" * 80)

for result in results:
    print(f"{result['algorithm']:<20} ${result['rmse']:<11,.0f} {result['r2']:<7.3f} ${result['mae']:<11,.0f} {result['training_time']:<7.2f}")

print("=" * 80)

# FIND BEST MODEL
best_rmse = min(results, key=lambda x: x['rmse'])
best_r2 = max(results, key=lambda x: x['r2'])
fastest = min(results, key=lambda x: x['training_time'])

print(f"\n🏆 WINNERS:")
print(f"   🎯 Best RMSE (lowest error): {best_rmse['algorithm']} (${best_rmse['rmse']:,.0f})")
print(f"   📈 Best R² (most variation explained): {best_r2['algorithm']} ({best_r2['r2']:.3f})")
print(f"   ⚡ Fastest training: {fastest['algorithm']} ({fastest['training_time']:.2f}s)")

# RECOMMENDATION
print(f"\n💡 RECOMMENDATION:")
if best_rmse['algorithm'] == best_r2['algorithm']:
    print(f"   🥇 Clear winner: {best_rmse['algorithm']}")
    print(f"   Reason: Best on both RMSE and R² metrics")
else:
    print(f"   🤔 Trade-off between accuracy and other factors")
    print(f"   For accuracy: {best_rmse['algorithm']}")
    print(f"   For speed: {fastest['algorithm']}")

print(f"\n🔍 DETAILED ANALYSIS:")
for result in results:
    print(f"\n{result['algorithm']}:")
    print(f"   ✅ Pros: {result['pros']}")
    print(f"   ⚠️ Cons: {result['cons']}")
    
    if result['rmse'] == min(r['rmse'] for r in results):
        print(f"   🏆 BEST accuracy!")
    if result['training_time'] == min(r['training_time'] for r in results):
        print(f"   ⚡ FASTEST training!")

print(f"\n🎯 CHOOSING THE RIGHT ALGORITHM:")
decision_guide = """
✅ Choose Linear Regression when:
   • You need interpretability (understand feature impacts)
   • You have linear relationships in data
   • You need fast predictions
   • You have limited training data

✅ Choose Decision Tree when:
   • You need some interpretability
   • You have non-linear patterns
   • You want to see decision rules

✅ Choose Random Forest when:
   • You want good accuracy with less overfitting
   • You need feature importance rankings
   • You have enough data (hundreds+ samples)
   • Interpretability is not critical

✅ Choose Gradient Boosted Trees when:
   • You need maximum accuracy
   • You have complex patterns in data
   • Training time is not a concern
   • You have sufficient data to avoid overfitting
"""
print(decision_guide)

print(f"\n⚠️ IMPORTANT NOTES:")
notes = """
📊 Small dataset warning: 
   • Our sample has only ~12 houses
   • Results may vary significantly with different train/test splits
   • Use larger datasets (1000+ samples) for reliable comparisons

🔄 For production use:
   • Cross-validate results (multiple train/test splits)
   • Tune hyperparameters (tree depth, learning rate, etc.)
   • Consider ensemble methods (combine multiple models)
   • Validate on completely separate dataset
"""
print(notes)

print(f"\n🚀 NEXT STEPS:")
next_steps = """
1️⃣ Try with your own, larger dataset
2️⃣ Experiment with hyperparameter tuning
3️⃣ Use cross-validation for robust evaluation
4️⃣ Consider feature engineering (polynomial features, etc.)
5️⃣ Implement the best model in production pipeline
"""
print(next_steps)

print("\n✅ Example 10 complete! You now know how to compare ML algorithms systematically.")
print("🎉 CONGRATULATIONS! You've mastered the complete Spark ML workflow!")
print("💡 You're ready to tackle real-world machine learning projects with Spark!")