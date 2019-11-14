# django-pandasio

**Install**

```bash
pip install git+https://github.com/NikitaZakharov/django-pandasio.git
```

**Example**

```python
import pandas as pd
import pandasio

from models import Product, Category


class ProductSerializer(pandasio.DataFrameSerializer):

    product_id = pandasio.CharField(max_length=100, source='identififer')
    name = pandasio.CharField(max_length=200)
    category_id = pandasio.IntegerField(required=False, allow_null=True)

    def validate_category_id(self, column):
        # isinstance(column, pd.Series) is True
        if column.isnull().any():
            root_category_id = Category.get_root_category_id()
            column = column.apply(lambda x: x if not pd.isnull(x) else root_category_id)
        return column

    class Meta:
        # django model used to save dataframe into a database
        model = Product
        validators = [
            pandasio.UniqueTogetherValidator(['product_id'])
        ]


dataframe = pd.DataFrame(
    data=[
        ['234556', 'Coca-Cola'],
        ['456454', 'Pepsi']
    ],
    columns=['product_id', 'name']
)

serializer = ProductSerializer(data=dataframe)
if serializer.is_valid():
    serializer.save()
```
