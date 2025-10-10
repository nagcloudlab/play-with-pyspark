
# Import necessary Spark modules
from pyspark.sql import SparkSession          # Main entry point for Spark
from pyspark.ml.feature import VectorAssembler # Combines multiple columns into one vector
from pyspark.ml.regression import LinearRegression # Linear regression algorithm
from pyspark.ml.evaluation import RegressionEvaluator # Evaluates model performance

# Start Spark in local mode
spark = SparkSession.builder \
    .appName("simple ML") \
    .master("local[*]") \
    .getOrCreate()                           
    
print("✅ Spark session created successfully!")
print(f"Spark version: {spark.version}")    

# Create sample house data manually (in real world, you'd load from CSV/database)
# Format: (area_sqft, bedrooms, bathrooms, age_years, price_dollars)
data = spark.createDataFrame([
    (1500, 2, 1, 10, 300000),    # Small house, older
    (2000, 3, 2, 5, 450000),     # Medium house, newer
    (1200, 1, 1, 15, 250000),    # Tiny house, old
    (2500, 4, 3, 2, 600000),     # Large house, very new
    (1800, 2, 2, 8, 380000),     # Medium house
    (1600, 2, 1, 12, 320000),    # Small-medium house
    (2200, 3, 2, 6, 480000),     # Large house, newish
    (1000, 1, 1, 20, 200000)     # Very small, very old
], ["area", "bedrooms", "bathrooms", "age", "price"])  # Column names



print("\n📊 Sample House Data:")
data.show()  # Display the DataFrame in tabular format

print("\n🔍 Data Schema (column types):")
data.printSchema()

print(f"\n📈 Total houses in dataset: {data.count()}")



print("🔧 Starting Feature Engineering...")

# ML algorithms expect ALL input features in a SINGLE column as a vector
# VectorAssembler combines multiple columns into one "features" vector
assembler = VectorAssembler(
    inputCols=["area", "bedrooms", "bathrooms", "age"],  # Input: separate columns
    outputCol="features"                                  # Output: combined vector column
)

print("✅ VectorAssembler created!")
print("Input columns:", assembler.getInputCols())
print("Output column:", assembler.getOutputCol())

# Transform the data - this adds the "features" column
# Original columns remain, new "features" column is added
feature_data = assembler.transform(data)

print("\n📊 Data with Features Vector:")
feature_data.select("features", "price").show()
# Output will look like: features=[1500.0, 2.0, 1.0, 10.0], price=300000

print("\n🔍 Full data with all columns:")
feature_data.show()

print("\n📋 What happened:")
print("✅ Combined 4 separate columns into 1 vector column")
print("✅ Each row now has a 'features' vector: [area, bedrooms, bathrooms, age]")
print("✅ ML algorithms can now use this vector as input")
print("✅ Original columns are still there (not deleted)")

# Let's examine one specific row to understand the vector
first_row = feature_data.first()
print(f"\n🔎 Example - First house:")
print(f"   Original: area={first_row['area']}, bedrooms={first_row['bedrooms']}, bathrooms={first_row['bathrooms']}, age={first_row['age']}")
print(f"   Vector: {first_row['features']}")
print(f"   Price: ${first_row['price']:,}")


print("✂️ Starting Train-Test Split...")

# Split data randomly: 80% for training, 20% for testing
# seed=42 ensures we get the same split every time (reproducible results)
train_data, test_data = feature_data.randomSplit([0.8, 0.2], seed=42)

# Check how many samples we got in each set
train_count = train_data.count()
test_count = test_data.count()
total_count = feature_data.count()

print(f"📊 Data Split Results:")
print(f"   Total samples: {total_count}")
print(f"   Training samples: {train_count} ({train_count/total_count*100:.1f}%)")
print(f"   Test samples: {test_count} ({test_count/total_count*100:.1f}%)")

print(f"\n🏠 Training Data (what the model will learn from):")
train_data.select("features", "price").show()

print(f"\n🔍 Test Data (what we'll use to check accuracy):")
test_data.select("features", "price").show()

print("\n📋 Why do we split the data?")
print("✅ TRAINING SET: Model learns patterns from this data")
print("✅ TEST SET: We check if model works on UNSEEN data") 
print("✅ This prevents overfitting (memorizing instead of learning)")
print("✅ Gives us honest estimate of real-world performance")

print("\n💡 The Golden Rule:")
print("🚫 NEVER let the model see test data during training!")
print("✅ Test data must stay 'unseen' until final evaluation")

# Let's see what houses ended up in each set
print(f"\n🔎 Training set price range: ${train_data.agg({'price': 'min'}).collect()[0][0]:,} to ${train_data.agg({'price': 'max'}).collect()[0][0]:,}")
print(f"🔎 Test set price range: ${test_data.agg({'price': 'min'}).collect()[0][0]:,} to ${test_data.agg({'price': 'max'}).collect()[0][0]:,}")



print("🧠 Starting Model Training...")

# Create a Linear Regression model
lr = LinearRegression(
    featuresCol="features",  # Input: the vector column we created
    labelCol="price"         # Output: what we want to predict
)

print("✅ Linear Regression model created!")
print(f"   Features column: {lr.getFeaturesCol()}")
print(f"   Label column: {lr.getLabelCol()}")

# Train the model using ONLY the training data
# .fit() learns the relationship between features and price
print("\n🔄 Training the model...")
model = lr.fit(train_data)

print("✅ Model training complete!")

# The model learns coefficients (weights) for each feature
coefficients = model.coefficients
intercept = model.intercept

print(f"\n📊 What the model learned:")
print(f"   Coefficients: {[round(c, 2) for c in coefficients]}")
print(f"   Intercept: {round(intercept, 2)}")

# Let's interpret what these numbers mean
feature_names = ["area", "bedrooms", "bathrooms", "age"]
print(f"\n🔍 Model interpretation:")
for i, (name, coef) in enumerate(zip(feature_names, coefficients)):
    direction = "increases" if coef > 0 else "decreases"
    print(f"   {name}: {coef:.2f} -> Each unit {direction} price by ${abs(coef):,.2f}")

print(f"   Base price (intercept): ${intercept:,.2f}")

# Show the learned equation
print(f"\n📐 The model's price prediction formula:")
equation_parts = []
for i, (name, coef) in enumerate(zip(feature_names, coefficients)):
    sign = "+" if coef >= 0 else ""
    equation_parts.append(f"{sign}{coef:.1f} × {name}")

equation = f"price = {' '.join(equation_parts)} + {intercept:.1f}"
print(f"   {equation}")

print(f"\n💡 Example prediction calculation:")
print(f"   For a house: 1800 sqft, 3 bed, 2 bath, 5 years old")
example_features = [1800, 3, 2, 5]
manual_prediction = sum(c * f for c, f in zip(coefficients, example_features)) + intercept
print(f"   Predicted price = {coefficients[0]:.1f}×1800 + {coefficients[1]:.1f}×3 + {coefficients[2]:.1f}×2 + {coefficients[3]:.1f}×5 + {intercept:.1f}")
print(f"   Predicted price = ${manual_prediction:,.2f}")




print("🔮 Starting Predictions...")

# Use the trained model to predict prices on test data
# .transform() applies the model to new data and adds a "prediction" column
print("🔄 Applying model to test data...")
predictions = model.transform(test_data)

print("✅ Predictions complete!")

print("\n📊 Predictions vs Actual Prices:")
predictions.select("features", "price", "prediction").show()

# Let's make the comparison easier to read
print("\n🔍 Detailed Comparison:")
prediction_results = predictions.select("features", "price", "prediction").collect()

for i, row in enumerate(prediction_results):
    features = row['features']
    actual = row['price']
    predicted = row['prediction']
    error = abs(actual - predicted)
    error_percent = (error / actual) * 100
    
    print(f"\n🏠 House {i+1}:")
    print(f"   Features: {[int(f) for f in features]} (area, bed, bath, age)")
    print(f"   Actual price: ${actual:,.2f}")
    print(f"   Predicted price: ${predicted:,.2f}")
    print(f"   Prediction error: ${error:,.2f} ({error_percent:.1f}%)")
    
    if error_percent < 10:
        print("   ✅ Excellent prediction!")
    elif error_percent < 20:
        print("   👍 Good prediction!")
    elif error_percent < 30:
        print("   ⚠️ Fair prediction")
    else:
        print("   ❌ Poor prediction")

print(f"\n📋 What happened behind the scenes:")
print("✅ Model took each house's features vector")
print("✅ Applied the learned formula: coef1×area + coef2×bedrooms + ... + intercept")
print("✅ Generated a predicted price")
print("✅ Added 'prediction' column to the DataFrame")

print(f"\n💡 Key insights:")
if len(prediction_results) > 0:
    avg_error = sum(abs(row['price'] - row['prediction']) for row in prediction_results) / len(prediction_results)
    print(f"   Average prediction error: ${avg_error:,.2f}")
    
    total_actual = sum(row['price'] for row in prediction_results)
    avg_actual = total_actual / len(prediction_results)
    print(f"   Average house price in test set: ${avg_actual:,.2f}")
    
    print(f"   Model accuracy: {100 - (avg_error/avg_actual*100):.1f}%")

print(f"\n🚀 This is how you'd use the model in real life:")
print("✅ New house comes on market")
print("✅ Input its features [area, bedrooms, bathrooms, age]")
print("✅ Model instantly predicts the price")
print("✅ No need to retrain - just apply the learned formula!")




print("📏 Starting Model Evaluation...")

# Calculate RMSE (Root Mean Squared Error)
# RMSE measures average prediction error in same units as target (dollars)
rmse_evaluator = RegressionEvaluator(
    labelCol="price",           # Actual values
    predictionCol="prediction", # Predicted values  
    metricName="rmse"          # Type of error metric
)

rmse = rmse_evaluator.evaluate(predictions)
print(f"📊 Root Mean Squared Error (RMSE): ${rmse:,.2f}")

# Calculate R-squared (coefficient of determination)
# R² tells us how much of price variation our model explains (0 to 1, higher is better)
r2_evaluator = RegressionEvaluator(
    labelCol="price", 
    predictionCol="prediction", 
    metricName="r2"
)

r2 = r2_evaluator.evaluate(predictions)
print(f"📊 R-squared (R²): {r2:.3f}")

# Calculate MAE (Mean Absolute Error)
mae_evaluator = RegressionEvaluator(
    labelCol="price", 
    predictionCol="prediction", 
    metricName="mae"
)

mae = mae_evaluator.evaluate(predictions)
print(f"📊 Mean Absolute Error (MAE): ${mae:,.2f}")

print(f"\n🔍 What these metrics mean:")

print(f"\n📈 RMSE (${rmse:,.2f}):")
print(f"   • Average prediction error in dollars")
print(f"   • Lower is better (perfect model = $0)")
print(f"   • Penalizes large errors more heavily")
if rmse < 50000:
    print(f"   ✅ Excellent! Predictions are very accurate")
elif rmse < 100000:
    print(f"   👍 Good! Reasonable prediction accuracy")
elif rmse < 150000:
    print(f"   ⚠️ Fair. Model needs improvement")
else:
    print(f"   ❌ Poor. Model is not very accurate")

print(f"\n📊 R² ({r2:.3f}):")
print(f"   • How much price variation the model explains")
print(f"   • Range: 0.0 (terrible) to 1.0 (perfect)")
print(f"   • Current: Model explains {r2*100:.1f}% of price differences")
if r2 > 0.9:
    print(f"   ✅ Excellent! Model captures almost all patterns")
elif r2 > 0.7:
    print(f"   👍 Good! Model captures most patterns")
elif r2 > 0.5:
    print(f"   ⚠️ Fair. Model captures some patterns")
else:
    print(f"   ❌ Poor. Model doesn't capture patterns well")

print(f"\n💰 MAE (${mae:,.2f}):")
print(f"   • Average absolute error (ignores +/- direction)")
print(f"   • More intuitive than RMSE")
print(f"   • On average, predictions are off by ${mae:,.2f}")

# Compare RMSE vs MAE to understand error distribution
if rmse > mae * 1.5:
    print(f"\n⚠️ RMSE much larger than MAE suggests some very large errors")
else:
    print(f"\n✅ RMSE close to MAE suggests consistent error sizes")

# Calculate some additional insights
prediction_data = predictions.collect()
if prediction_data:
    actual_prices = [row['price'] for row in prediction_data]
    predicted_prices = [row['prediction'] for row in prediction_data]
    
    avg_actual = sum(actual_prices) / len(actual_prices)
    avg_predicted = sum(predicted_prices) / len(predicted_prices)
    
    print(f"\n🏠 Price Analysis:")
    print(f"   Average actual price: ${avg_actual:,.2f}")
    print(f"   Average predicted price: ${avg_predicted:,.2f}")
    print(f"   Prediction bias: ${avg_predicted - avg_actual:,.2f}")
    
    if abs(avg_predicted - avg_actual) < avg_actual * 0.05:
        print(f"   ✅ Model is well-calibrated (no systematic bias)")
    else:
        bias_direction = "over-estimating" if avg_predicted > avg_actual else "under-estimating"
        print(f"   ⚠️ Model is {bias_direction} prices on average")

print(f"\n🎯 Model Performance Summary:")
print(f"   • Typical error: ±${mae:,.0f}")
print(f"   • Explains {r2*100:.0f}% of price variation")
print(f"   • RMSE: ${rmse:,.0f}")

if r2 > 0.8 and rmse < 50000:
    print(f"   🏆 EXCELLENT model - ready for production!")
elif r2 > 0.6 and rmse < 100000:
    print(f"   👍 GOOD model - useful for predictions")
elif r2 > 0.4:
    print(f"   ⚠️ FAIR model - needs improvement")
else:
    print(f"   ❌ POOR model - need more data or different approach")

print(f"\n💡 Next steps to improve model:")
print(f"   • Add more features (location, garage, etc.)")
print(f"   • Collect more training data")
print(f"   • Try different algorithms (Random Forest, etc.)")
print(f"   • Feature engineering (area per bedroom, etc.)")




print("🔮 Predicting Price for a New House...")

# Scenario: A new house just came on the market!
# We want to estimate its price using our trained model

print("\n🏠 New house details:")
print("   Area: 1900 sq ft")
print("   Bedrooms: 3")
print("   Bathrooms: 2") 
print("   Age: 7 years")


# Create data for the brand new house (not in our training/test data)
new_house = spark.createDataFrame([
    (1900, 3, 2, 7)  # area, bedrooms, bathrooms, age
], ["area", "bedrooms", "bathrooms", "age"])


print("\n📊 New house as DataFrame:")
new_house.show()

# Apply the SAME feature engineering we used during training
# This is crucial - new data must be processed identically to training data
print("\n🔧 Applying feature engineering...")
new_house_features = assembler.transform(new_house)

print("✅ Features vector created:")
new_house_features.select("features").show()

# Make prediction using our trained model
print("\n🔄 Making prediction...")
new_prediction = model.transform(new_house_features)

print("✅ Prediction complete!")
new_prediction.select("area", "bedrooms", "bathrooms", "age", "features", "prediction").show()

# Extract the predicted price from the DataFrame
predicted_price = new_prediction.select("prediction").collect()[0][0]

print(f"\n💰 PREDICTION RESULT:")
print(f"   🏠 House: 1900 sq ft, 3 bed, 2 bath, 7 years old")
print(f"   💵 Predicted price: ${predicted_price:,.2f}")

# Let's also show the manual calculation to understand what happened
coefficients = model.coefficients
intercept = model.intercept
features = [1900, 3, 2, 7]

print(f"\n🧮 Manual calculation (to verify):")
calculation_parts = []
total = intercept
print(f"   Base price (intercept): ${intercept:,.2f}")

feature_names = ["area", "bedrooms", "bathrooms", "age"]
for i, (name, feature_val, coef) in enumerate(zip(feature_names, features, coefficients)):
    contribution = coef * feature_val
    total += contribution
    sign = "+" if contribution >= 0 else ""
    print(f"   {name}: {coef:.2f} × {feature_val} = {sign}${contribution:,.2f}")

print(f"   ─────────────────────────────")
print(f"   Total predicted price: ${total:,.2f}")
print(f"   (Should match Spark result: ${predicted_price:,.2f})")

# Compare with similar houses from our training data
print(f"\n🔍 Comparison with similar houses from training:")
similar_houses = train_data.filter(
    (train_data.area.between(1700, 2100)) &  # Similar size
    (train_data.bedrooms == 3)                # Same bedrooms
).select("area", "bedrooms", "bathrooms", "age", "price")

if similar_houses.count() > 0:
    print("   Similar houses in training data:")
    similar_houses.show()
    
    avg_similar_price = similar_houses.agg({"price": "avg"}).collect()[0][0]
    print(f"   Average price of similar houses: ${avg_similar_price:,.2f}")
    print(f"   Our prediction: ${predicted_price:,.2f}")
    difference = predicted_price - avg_similar_price
    print(f"   Difference: ${difference:,.2f}")
else:
    print("   No very similar houses in training data")

print(f"\n🚀 Real-world usage:")
print(f"   ✅ This is exactly how you'd use the model in production")
print(f"   ✅ Real estate app: user inputs house details → instant price estimate")
print(f"   ✅ Bank loan approval: quick property valuation")
print(f"   ✅ Investment analysis: evaluate potential purchases")

print(f"\n💡 Key points:")
print(f"   ✅ Model works on ANY new house (not just test data)")
print(f"   ✅ Same preprocessing must be applied (VectorAssembler)")
print(f"   ✅ Prediction is instant (no retraining needed)")
print(f"   ✅ Model generalizes beyond the original dataset")

# Let's predict a few more houses to see the model in action
print(f"\n🏠 Let's predict a few more houses:")

more_houses = spark.createDataFrame([
    (2500, 4, 3, 3),   # Large, new house
    (1200, 2, 1, 15),  # Small, older house  
    (3000, 5, 4, 1)    # Very large, very new house
], ["area", "bedrooms", "bathrooms", "age"])

more_features = assembler.transform(more_houses)
more_predictions = model.transform(more_features)

print("   Batch predictions:")
more_predictions.select("area", "bedrooms", "bathrooms", "age", "prediction").show()

batch_results = more_predictions.select("area", "bedrooms", "bathrooms", "age", "prediction").collect()
for i, row in enumerate(batch_results):
    print(f"   House {i+1}: {row['area']} sqft, {row['bedrooms']} bed → ${row['prediction']:,.0f}")

print("\n✅ Example 7 complete! Model successfully predicted prices for new houses.")
print("💡 Next: Learn how to wrap everything in a Spark Pipeline for production use!")





# ========================================
# Example 8: Spark Pipeline
# ========================================

# NOTE: This can be run independently, but works best after Examples 1-7
# We'll use the same data but show the PROFESSIONAL way to do ML in Spark

print("🔄 Building a Spark Pipeline...")

from pyspark.ml import Pipeline

# A Pipeline chains multiple steps together into one workflow
# This is the PRODUCTION-READY way to do machine learning

print("\n🧩 What is a Pipeline?")
print("   📦 Bundles preprocessing + modeling into ONE object")
print("   🔒 Ensures same steps are always applied in same order")
print("   🚀 Makes deployment easier - save/load entire workflow")
print("   🛡️ Prevents data leakage and preprocessing errors")

# Create individual pipeline stages (same as before, but organized)
print("\n🏗️ Creating pipeline stages...")

# Stage 1: Feature Engineering
assembler = VectorAssembler(
    inputCols=["area", "bedrooms", "bathrooms", "age"],
    outputCol="features"
)
print("   ✅ Stage 1: VectorAssembler (feature engineering)")

# Stage 2: Machine Learning Algorithm  
lr = LinearRegression(
    featuresCol="features", 
    labelCol="price"
)
print("   ✅ Stage 2: LinearRegression (model training)")

# Create the Pipeline by chaining stages together
pipeline = Pipeline(stages=[
    assembler,  # Step 1: Convert columns to features vector
    lr          # Step 2: Apply linear regression
])

print("\n🔗 Pipeline created with 2 stages:")
print("   1️⃣ VectorAssembler → creates features vector")
print("   2️⃣ LinearRegression → learns price prediction")

print("\n📊 Using the same data from previous examples...")
# We'll use the feature_data from earlier examples, but let's recreate it for clarity
if 'data' not in locals():
    # If running independently, recreate the data
    data = spark.createDataFrame([
        (1500, 2, 1, 10, 300000),
        (2000, 3, 2, 5, 450000),
        (1200, 1, 1, 15, 250000),
        (2500, 4, 3, 2, 600000),
        (1800, 2, 2, 8, 380000),
        (1600, 2, 1, 12, 320000),
        (2200, 3, 2, 6, 480000),
        (1000, 1, 1, 20, 200000)
    ], ["area", "bedrooms", "bathrooms", "age", "price"])

# Split the data
train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)
print(f"   Training samples: {train_data.count()}")
print(f"   Test samples: {test_data.count()}")

# Fit the entire pipeline on training data
print("\n🔄 Training the complete pipeline...")
pipeline_model = pipeline.fit(train_data)

print("✅ Pipeline training complete!")
print("   🎯 Both VectorAssembler and LinearRegression are now trained")
print("   📦 Everything bundled into one 'pipeline_model' object")

# Make predictions using the complete pipeline
print("\n🔮 Making predictions with pipeline...")
pipeline_predictions = pipeline_model.transform(test_data)

print("✅ Pipeline predictions complete!")
print("\n📊 Results:")
pipeline_predictions.select("area", "bedrooms", "bathrooms", "age", "price", "prediction").show()

# Evaluate the pipeline model (same as before)
evaluator = RegressionEvaluator(
    labelCol="price", 
    predictionCol="prediction", 
    metricName="rmse"
)

pipeline_rmse = evaluator.evaluate(pipeline_predictions)
print(f"\n📏 Pipeline RMSE: ${pipeline_rmse:,.2f}")

# The magic of pipelines: predict new data in ONE step
print("\n🎩 The Pipeline Magic:")
print("   Instead of manually doing:")
print("   1️⃣ new_data → VectorAssembler → features")
print("   2️⃣ features → LinearRegression → predictions")
print("   ")
print("   Pipeline does EVERYTHING in one step:")
print("   🚀 new_data → Pipeline → predictions")

# Demonstrate with a new house
new_house = spark.createDataFrame([
    (2100, 3, 2, 4)  # 2100 sqft, 3 bed, 2 bath, 4 years old
], ["area", "bedrooms", "bathrooms", "age"])

print(f"\n🏠 Testing pipeline with new house:")
new_house.show()

# ONE command does everything: preprocessing + prediction
new_house_prediction = pipeline_model.transform(new_house)

print("✅ Pipeline result (one step!):")
new_house_prediction.select("area", "bedrooms", "bathrooms", "age", "features", "prediction").show()

predicted_price = new_house_prediction.select("prediction").collect()[0][0]
print(f"💰 Predicted price: ${predicted_price:,.2f}")

print(f"\n🏆 Benefits of Pipelines:")
print("✅ NO manual preprocessing - pipeline handles it automatically")
print("✅ NO risk of forgetting steps or doing them wrong")
print("✅ CONSISTENT processing between training and prediction")
print("✅ EASY to save/load entire workflow")
print("✅ PRODUCTION-READY - scales to millions of records")

# Show how to save/load pipeline (commented out to avoid file system issues)
print(f"\n💾 Saving/Loading Pipelines (for production):")
print("   # Save the trained pipeline")
print("   # pipeline_model.write().overwrite().save('house_price_model')")
print("   ")
print("   # Load the pipeline later")
print("   # from pyspark.ml import PipelineModel")
print("   # loaded_model = PipelineModel.load('house_price_model')")
print("   # predictions = loaded_model.transform(new_data)")

print(f"\n🔄 Pipeline vs Manual Approach:")
print("📊 Manual (Examples 1-7):")
print("   ❌ Multiple steps, easy to make mistakes")
print("   ❌ Must remember exact preprocessing sequence")
print("   ❌ Risk of train/test preprocessing differences")
print("")
print("🚀 Pipeline (Example 8):")
print("   ✅ One object does everything")
print("   ✅ Impossible to forget preprocessing steps")
print("   ✅ Guaranteed consistency")
print("   ✅ Professional, production-ready approach")

# Compare results to make sure pipeline gives same answer
if 'predictions' in locals():
    manual_rmse = evaluator.evaluate(predictions)
    print(f"\n🎯 Verification:")
    print(f"   Manual approach RMSE: ${manual_rmse:,.2f}")
    print(f"   Pipeline approach RMSE: ${pipeline_rmse:,.2f}")
    if abs(manual_rmse - pipeline_rmse) < 0.01:
        print("   ✅ Results match perfectly!")
    else:
        print("   ⚠️ Small differences (normal due to randomization)")

print("\n🎓 When to use Pipelines:")
print("✅ ALWAYS use pipelines for production systems")
print("✅ Use for any serious ML project")  
print("✅ Use when you need to save/deploy models")
print("✅ Use to prevent preprocessing mistakes")
print("")
print("📚 Manual approach is good for:")
print("✅ Learning and understanding each step")
print("✅ Debugging and experimentation")
print("✅ Quick prototyping")

print("\n✅ Example 8 complete! You now know the professional way to do ML in Spark.")