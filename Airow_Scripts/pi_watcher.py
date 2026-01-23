import time
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from rembg import remove
from PIL import Image

# --- CONFIGURATION ---
PATH_TO_WATCH = "/home/pat/Desktop/Airow"
PROCESSED_FOLDER = os.path.join(PATH_TO_WATCH, "Ready_For_Print")
MOCKUP_FOLDER = os.path.join(PATH_TO_WATCH, "Marketing_Assets")
TEMPLATE_PATH = os.path.join(PATH_TO_WATCH, "shirt_template.jpg")

# Ensure folders exist
for folder in [PROCESSED_FOLDER, MOCKUP_FOLDER]:
    if not os.path.exists(folder):
        os.makedirs(folder)

class OnMyWatch:
    watchDirectory = PATH_TO_WATCH

    def __init__(self):
        self.observer = Observer()

    def run(self):
        event_handler = Handler()
        self.observer.schedule(event_handler, self.watchDirectory, recursive=True)
        self.observer.start()
        print(f"AIROW PI FACTORY: ONLINE")
        try:
            while True:
                time.sleep(5)
        except:
            self.observer.stop()
        self.observer.join()

class Handler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory: return
        filename = event.src_path
        
        # Filters to prevent loops
        if "Ready_For_Print" in filename or "Marketing_Assets" in filename: return
        if not (filename.lower().endswith(('.png', '.jpg', '.jpeg'))): return
        if "shirt_template.jpg" in filename: return

        # Wait 5 seconds for Syncthing to finish downloading the file completely
        time.sleep(5)

        print(f"\n[+] Processing: {os.path.basename(filename)}")
        
        try:
            # 1. REMOVE BACKGROUND
            print("    > Cleaning...")
            input_image = Image.open(filename)
            clean_image = remove(input_image)
            
            base_name = os.path.splitext(os.path.basename(filename))[0]
            clean_path = os.path.join(PROCESSED_FOLDER, f"{base_name}_CLEAN.png")
            clean_image.save(clean_path)

            # 2. CREATE MOCKUP (If template exists)
            if os.path.exists(TEMPLATE_PATH):
                print("    > Mocking up...")
                background = Image.open(TEMPLATE_PATH).convert("RGBA")
                bg_w, bg_h = background.size
                
                design = clean_image.convert("RGBA")
                
                # Resize to 35% of shirt width
                target_width = int(bg_w * 0.35) 
                aspect_ratio = design.height / design.width
                target_height = int(target_width * aspect_ratio)
                
                design_resized = design.resize((target_width, target_height), Image.Resampling.LANCZOS)
                
                # Center it
                width_offset = (bg_w - target_width) // 2
                height_offset = int(bg_h * 0.25)
                
                background.paste(design_resized, (width_offset, height_offset), design_resized)
                
                mockup_path = os.path.join(MOCKUP_FOLDER, f"{base_name}_MOCKUP.jpg")
                background.convert("RGB").save(mockup_path, quality=90)

        except Exception as e:
            print(f"    > ERROR: {e}")

if __name__ == '__main__':
    watch = OnMyWatch()
    watch.run()
