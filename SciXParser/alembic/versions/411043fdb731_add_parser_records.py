"""add_parser_records

Revision ID: 411043fdb731
Revises: 6bfebb4af6ba
Create Date: 2023-06-01 15:41:18.841799

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "411043fdb731"
down_revision = "6bfebb4af6ba"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "PARSER_records",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("s3_key", sa.String(), nullable=False),
        sa.Column("date_created", sa.DateTime(), nullable=True),
        sa.Column("date_modified", sa.DateTime(), nullable=True),
        sa.Column("parsed_data", sa.JSON(), nullable=True),
        sa.Column(
            "source",
            sa.Enum("ARXIV", "SYMBOL2", "SYMBOL3", "SYMBOL4", name="source"),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table("PARSER_records")
    op.execute("DROP TYPE source")
    # ### end Alembic commands ###