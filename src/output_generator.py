import pandas as pd

class OutputGenerator:
    def generate_excel(self, data, output_path):
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            for tab_name, df in data.items():
                df.to_excel(writer, sheet_name=tab_name, index=False)
                
                # Auto-adjust column widths
                worksheet = writer.sheets[tab_name]
                for idx, col in enumerate(df.columns):
                    max_length = max(
                        df[col].astype(str).apply(len).max(),
                        len(str(col))
                    )
                    # Get the column letter
                    column_letter = chr(65 + idx) if idx < 26 else chr(64 + idx // 26) + chr(65 + (idx % 26))
                    adjusted_width = min(max_length + 2, 50)  # Cap width at 50
                    worksheet.column_dimensions[column_letter].width = adjusted_width
