import os

root_dir = r"c:\Users\26481\DSA 2027\food-delivery-platform"

for root, dirs, files in os.walk(root_dir):
    for file in files:
        if file == "Config.docker.toml":
            path = os.path.join(root, file)
            print(f"Updating dbName in {path}")
            with open(path, "r", encoding="utf-8") as f:
                lines = f.readlines()
            
            new_lines = []
            for line in lines:
                if line.startswith("dbName"):
                    new_lines.append('dbName = "food_delivery_db"\n')
                else:
                    new_lines.append(line)
            
            with open(path, "w", encoding="utf-8") as f:
                f.writelines(new_lines)

print("Done updating dbName to food_delivery_db!")
