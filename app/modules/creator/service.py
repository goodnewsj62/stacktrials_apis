from typing import Optional

from fastapi import HTTPException
from sqlmodel import col, func, select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.common.constants import PER_PAGE
from app.common.enum import CourseStatus, VisibilityType
from app.common.utils import paginate
from app.models.courses_model import Course
from app.models.user_model import Account
from app.schemas.courses import CreatorStat


class CreatorService:
    @staticmethod
    async def course_stat(current_user: Account, session: AsyncSession):
        total_enrolled = (
            await session.exec(
                select(func.sum(Course.enrollment_count)).where(
                    Course.account_id == current_user.id
                )
            )
        ).one_or_none() or 0
        total_reviews = (
            await session.exec(
                select(func.sum(Course.total_rating)).where(
                    Course.account_id == current_user.id
                )
            )
        ).one_or_none() or 0
        total_comments = (
            await session.exec(
                select(func.sum(Course.comment_count)).where(
                    Course.account_id == current_user.id
                )
            )
        ).one_or_none() or 0
        average_rating = (
            await session.exec(
                select(func.avg(Course.average_rating)).where(
                    Course.account_id == current_user.id
                )
            )
        ).one_or_none()

        total_published = (
            await session.exec(
                select(func.count(col(Course.id))).where(
                    Course.status == CourseStatus.PUBLISHED,
                    Course.account_id == current_user.id,
                )
            )
        ).one_or_none() or 0

        print("____________________", average_rating)

        return CreatorStat(
            total_comments=total_comments,
            total_enrolled=total_enrolled,
            total_reviews=total_reviews,
            total_published=total_published,
        )

    @staticmethod
    async def created_videos(
        title: Optional[str],
        current_user: Account,
        session: AsyncSession,
        page: int = 1,
        per_page: int = PER_PAGE,
    ):
        query = select(Course).where(Course.account_id == current_user.id)
        if title:
            query = query.where(col(Course.title).ilike(f"%{title}%"))

        return await paginate(session, query, page, per_page)

    @staticmethod
    async def page_videos(
        title: Optional[str],
        username: str,
        session: AsyncSession,
        page: int = 1,
        per_page: int = PER_PAGE,
    ):

        user = (
            await session.exec(select(Account).where(Account.username == username))
        ).first()
        if not user:
            raise HTTPException(404, "user not found")

        query = select(Course).where(
            Course.account_id == user.id,
            Course.status == CourseStatus.PUBLISHED,
            Course.visibility == VisibilityType.PUBLIC,
        )
        if title:
            query = query.where(col(Course.title).ilike(f"%{title}%"))

        return await paginate(session, query, page, per_page)
