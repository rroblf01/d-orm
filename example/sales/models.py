import dorm
from dorm.contrib.pgvector import VectorField
from dorm.contrib.pgvector import L2Distance
from dorm.signals import post_save


class Customer(dorm.Model):
    name = dorm.CharField(max_length=100)
    email = dorm.EmailField(unique=True)
    phone = dorm.CharField(max_length=20, null=True, blank=True)


class Order(dorm.Model):
    customer = dorm.ForeignKey(Customer, on_delete=dorm.CASCADE)
    order_date = dorm.DateTimeField(auto_now_add=True)
    total = dorm.DecimalField(max_digits=10, decimal_places=2)


class Document(dorm.Model):
    title = dorm.CharField(max_length=200)
    embedding = VectorField(dimensions=384, null=True, blank=True)

    @staticmethod
    def find_similar_documents(query_embedding, top_k=5):
        return Document.objects.annotate(
            similarity=L2Distance("embedding", query_embedding)
        ).order_by("similarity")[:top_k]


async def update_document_embedding(sender, instance, **kwargs):
    # Simulate embedding generation (replace with actual embedding logic)
    if instance.embedding is None:
        instance.embedding = [0.0] * 384  # Replace with actual embedding vector
        await sender.objects.filter(id=instance.id).aupdate(
            embedding=instance.embedding
        )


post_save.connect(update_document_embedding, sender=Document)
