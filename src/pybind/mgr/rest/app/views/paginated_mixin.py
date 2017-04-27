from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from rest_framework.exceptions import ParseError
from rest_framework.pagination import PaginationSerializer


class PaginatedMixin(object):
    default_page_size = 10

    @property
    def _pagination_serializer(self):
        if not hasattr(self, '__pagination_serializer'):
            class LocalPaginationSerializer(PaginationSerializer):
                class Meta:
                    object_serializer_class = self.serializer_class

            self.__pagination_serializer = LocalPaginationSerializer
        return self.__pagination_serializer

    def _paginate(self, request, objects):
        # Pagination is, of course, separate to databaseyness, so you might think
        # to put this in a different mixin.  However, the *way* you do pagination
        # with LIMIT et al is rather coupled to the database, so here we are.

        page_number = request.GET.get('page', 1)
        page_size = request.GET.get('page_size', self.default_page_size)

        # The django paginator conveniently works for sqlalchemy querysets because
        # they both have .count() and support array slicing
        try:
            paginator = Paginator(objects, page_size)
            page = paginator.page(page_number)
        except (ValueError, EmptyPage, PageNotAnInteger) as e:
            # Raise 400 is 'page' or 'page_size' were bad
            raise ParseError(str(e))
        ps = self._pagination_serializer(instance=page, context={'request': request})
        return ps.data
